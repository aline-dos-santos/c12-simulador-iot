import random
import time
import threading
from queue import Queue, PriorityQueue, Empty
from dataclasses import dataclass, replace
from datetime import datetime

import tkinter as tk
from tkinter import ttk, messagebox

from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

from contextlib import closing

import sqlite3

SENTINELA = object()

@dataclass
class Configuracao:
    num_sensores: int
    threads_por_sensor: int
    leituras_por_sensor: int
    intervalo_leituras: float
    tempo_processamento: float
    num_servidores: int
    threads_por_servidor: int
    modo_sincronizador: str = "mutex"
    capacidade_semaforo: int = 1
    algoritmo: str = "FCFS"


class Database:
    def __init__(self, nome="leituras_sensores.db"):
        self.nome = nome
        with closing(self.conectar()) as conexao, conexao:
            conexao.execute("""CREATE TABLE IF NOT EXISTS leituras (
                leitura_id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id INTEGER NOT NULL,
                temperatura REAL NOT NULL,
                horario REAL NOT NULL,
                status TEXT NOT NULL,
                prioridade INTEGER NOT NULL
            )""")

    def conectar(self):
        return sqlite3.connect(self.nome, timeout=30)

    def reiniciar(self):
        # Cada teste utiliza apenas suas próprias leituras.
        with closing(self.conectar()) as conexao, conexao:
            conexao.execute("DELETE FROM leituras")

    def gravacao(self, leitura):
        with closing(self.conectar()) as conexao, conexao:
            conexao.execute("""INSERT INTO leituras
                (sensor_id, temperatura, horario, status, prioridade)
                VALUES (?, ?, ?, 'pendente', ?)""",
                (leitura["sensor_id"], leitura["temperatura"],
                 leitura["horario"], leitura["prioridade"]))

    def buscar_pendentes(self, limite=20):
        with closing(self.conectar()) as conexao:
            conexao.row_factory = sqlite3.Row
            return [dict(linha) for linha in conexao.execute("""
                SELECT * FROM leituras WHERE status = 'pendente'
                ORDER BY leitura_id LIMIT ?""", (limite,))]

    def reservar_leitura(self, leitura_id):
        # O UPDATE condicional impede reserva duplicada (pendente -> na_fila).
        with closing(self.conectar()) as conexao, conexao:
            cursor = conexao.execute("""UPDATE leituras
                SET status = 'na_fila'
                WHERE leitura_id = ? AND status = 'pendente'""",
                (leitura_id,))
            return cursor.rowcount == 1

    def iniciar_processamento(self, leitura_id):
        # O servidor pegou da fila e começa a trabalhar (na_fila -> processando).
        with closing(self.conectar()) as conexao, conexao:
            cursor = conexao.execute("""UPDATE leituras
                SET status = 'processando'
                WHERE leitura_id = ? AND status = 'na_fila'""",
                (leitura_id,))
            return cursor.rowcount == 1

    def atualizar_status(self, leitura_id, novo_status):
        if novo_status not in ("pendente", "na_fila", "processando", "concluido"):
            raise ValueError("Status inválido")
        with closing(self.conectar()) as conexao, conexao:
            conexao.execute("UPDATE leituras SET status = ? WHERE leitura_id = ?",
                            (novo_status, leitura_id))

    def concluir_leitura(self, leitura_id):
        with closing(self.conectar()) as conexao, conexao:
            cursor = conexao.execute("""UPDATE leituras SET status = 'concluido'
                WHERE leitura_id = ? AND status = 'processando'""",
                (leitura_id,))
            return cursor.rowcount == 1

    def devolver_leitura(self, leitura_id):
        with closing(self.conectar()) as conexao, conexao:
            conexao.execute("""UPDATE leituras SET status = 'pendente'
                WHERE leitura_id = ? AND status IN ('na_fila', 'processando')""",
                (leitura_id,))

    def contagem_status(self):
        with closing(self.conectar()) as conexao:
            return dict(conexao.execute(
                "SELECT status, COUNT(*) FROM leituras GROUP BY status").fetchall())


class Sincronizador:
    def __init__(self, modo="mutex", n_semaf=1):
        self.modo = modo
        if modo == "mutex":
            self.recurso = threading.Lock()
        elif modo == "semaforo":
            if n_semaf < 1:
                raise ValueError("Semáforo deve ter capacidade >= 1")
            self.recurso = threading.Semaphore(n_semaf)
        elif modo == "nenhum":
            self.recurso = None
        else:
            raise ValueError("Modo de sincronização inválido")
        self.lock_metricas = threading.Lock()
        self.bloqueios = 0
        self.tempo_espera = 0.0

    def adquirir(self):
        if self.recurso is None:
            return
        inicio = time.perf_counter()
        if not self.recurso.acquire(blocking=False):
            print(f"[{threading.current_thread().name}] BLOQUEADA")
            self.recurso.acquire()
            with self.lock_metricas:
                self.bloqueios += 1
                self.tempo_espera += time.perf_counter() - inicio

    def liberar(self):
        if self.recurso is not None:
            self.recurso.release()

    def obter_metricas(self):
        with self.lock_metricas:
            return {"bloqueios": self.bloqueios,
                    "tempo_espera": self.tempo_espera}


class ContadorCompartilhado:
    def __init__(self):
        self.valor = 0

    def incrementar(self):
        # Intencionalmente NÃO atômico para demonstrar lost update.
        atual = self.valor
        time.sleep(0.0005)
        self.valor = atual + 1


class IotSensor:
    def __init__(self, sensor_id, banco, sincronizador, contador):
        self.sensor_id = sensor_id
        self.banco = banco
        self.sincronizador = sincronizador
        self.contador = contador

    def gera_medida(self):
        return {"sensor_id": self.sensor_id,
                "temperatura": random.uniform(15, 35),
                "horario": time.time(), "prioridade": random.randint(1, 3)}

    def leitura(self, quantidade, intervalo, stop_event, medidas_fixas=None):
        for indice in range(quantidade):
            if stop_event.is_set():
                break
            medida = (dict(medidas_fixas[indice]) if medidas_fixas is not None
                      else self.gera_medida())
            if medidas_fixas is not None:
                medida["horario"] = time.time()
            self.sincronizador.adquirir()
            try:
                self.banco.gravacao(medida)
                self.contador.incrementar()
            finally:
                self.sincronizador.liberar()
            if stop_event.wait(intervalo):
                break


class ColetorResultados:
    def __init__(self, guardar_leituras=True):
        self.guardar_leituras = guardar_leituras
        self.resultados = []
        self.total = 0
        self.espera_total = 0.0
        self.esperas_prioridade = {1: [0.0, 0], 2: [0.0, 0], 3: [0.0, 0]}
        self.lock = threading.Lock()

    def adicionar(self, leitura):
        with self.lock:
            self.total += 1
            espera = leitura["espera_processamento"]
            self.espera_total += espera
            grupo = self.esperas_prioridade[leitura["prioridade"]]
            grupo[0] += espera
            grupo[1] += 1
            if self.guardar_leituras:
                self.resultados.append(leitura)


class Servidor:
    def __init__(self, servidor_id, fila, banco, tempo_processamento,
                 coletor, stop_event):
        self.servidor_id = servidor_id
        self.fila = fila
        self.banco = banco
        self.tempo_processamento = tempo_processamento
        self.coletor = coletor
        self.stop_event = stop_event

    def run(self):
        while True:
            leitura = self.fila.get()
            try:
                if isinstance(leitura, tuple):  # PriorityQueue: prioridade, id, leitura
                    leitura = leitura[2]
                if leitura is SENTINELA:
                    return
                leitura_id = leitura["leitura_id"]
                inicio_processamento = time.perf_counter()
                # na_fila -> processando: só agora o servidor realmente trabalha.
                if not self.banco.iniciar_processamento(leitura_id):
                    continue
                if self.stop_event.wait(self.tempo_processamento):
                    self.banco.devolver_leitura(leitura_id)
                    continue
                if self.banco.concluir_leitura(leitura_id):
                    resultado = dict(leitura)
                    resultado["espera_processamento"] = max(
                        0.0, inicio_processamento - leitura["enfileirado_em"])
                    resultado["status"] = "concluido"
                    resultado["servidor_id"] = self.servidor_id
                    resultado["thread_servidor"] = threading.current_thread().name
                    self.coletor.adicionar(resultado)
            finally:
                self.fila.task_done()


def despachar_pendentes(banco, fila, stop_event, fim_produtores, algoritmo="FCFS"):
    # FCFS: ordem de chegada; Prioridade: menor número primeiro.
    while not stop_event.is_set():
        pendentes = banco.buscar_pendentes(limite=20)
        for leitura in pendentes:
            if stop_event.is_set():
                break
            if banco.reservar_leitura(leitura["leitura_id"]):
                leitura["enfileirado_em"] = time.perf_counter()
                if algoritmo == "Prioridade":
                    fila.put((leitura["prioridade"], leitura["leitura_id"], leitura))
                else:
                    fila.put(leitura)
        if fim_produtores.is_set() and not banco.buscar_pendentes(limite=1):
            break
        stop_event.wait(0.05)


def dividir_trabalho(total, quantidade_threads):
    base, resto = divmod(total, quantidade_threads)
    return [base + (i < resto) for i in range(quantidade_threads)]


def executar_simulacao(config, stop_event=None, guardar_leituras=True,
                       callback_estados=None, carga=None):
    stop_event = stop_event if stop_event is not None else threading.Event()
    banco = Database()
    banco.reiniciar()
    fila = PriorityQueue() if config.algoritmo == "Prioridade" else Queue()
    fim_produtores = threading.Event()
    sincronizador = Sincronizador(config.modo_sincronizador,
                                  config.capacidade_semaforo)
    contador = ContadorCompartilhado()
    coletor = ColetorResultados(guardar_leituras)
    total_esperado = config.num_sensores * config.leituras_por_sensor
    total_threads_sensores = config.num_sensores * config.threads_por_sensor
    total_threads_servidor = config.num_servidores * config.threads_por_servidor

    # Monitor opcional: publica a contagem de estados periodicamente.
    parar_monitor = threading.Event()
    monitor = None
    if callback_estados is not None:
        def monitorar():
            while not parar_monitor.is_set():
                callback_estados(banco.contagem_status())
                parar_monitor.wait(0.1)
        monitor = threading.Thread(target=monitorar, name="Monitor", daemon=True)

    threads_servidores = []
    for servidor_id in range(config.num_servidores):
        servidor = Servidor(servidor_id, fila, banco, config.tempo_processamento,
                            coletor, stop_event)
        for i in range(config.threads_por_servidor):
            threads_servidores.append(threading.Thread(
                target=servidor.run, name=f"Servidor-{servidor_id}-Thread-{i}"))

    threads_sensores = []
    for sensor_id in range(config.num_sensores):
        sensor = IotSensor(sensor_id, banco, sincronizador, contador)
        medidas = carga[sensor_id] if carga is not None else None
        inicio_fatia = 0
        for i, quantidade in enumerate(dividir_trabalho(
                config.leituras_por_sensor, config.threads_por_sensor)):
            parte = medidas[inicio_fatia:inicio_fatia + quantidade] if medidas is not None else None
            inicio_fatia += quantidade
            threads_sensores.append(threading.Thread(
                target=sensor.leitura,
                args=(quantidade, config.intervalo_leituras, stop_event, parte),
                name=f"Sensor-{sensor_id}-Thread-{i}"))

    inicio = time.perf_counter()
    if monitor:
        monitor.start()
    for thread in threads_servidores:
        thread.start()
    despachante = threading.Thread(
        target=despachar_pendentes,
        args=(banco, fila, stop_event, fim_produtores, config.algoritmo),
        name="Despachante")
    despachante.start()
    for thread in threads_sensores:
        thread.start()
    for thread in threads_sensores:
        thread.join()
    tempo_geracao = time.perf_counter() - inicio
    fim_produtores.set()
    despachante.join()
    for _ in threads_servidores:
        if config.algoritmo == "Prioridade":
            fila.put((float("inf"), float("inf"), SENTINELA))
        else:
            fila.put(SENTINELA)
    for thread in threads_servidores:
        thread.join()

    tempo_total = time.perf_counter() - inicio
    parar_monitor.set()
    if monitor:
        monitor.join()
        callback_estados(banco.contagem_status())  # estado final na tela
    estados = banco.contagem_status()
    gravadas = sum(estados.values())
    metricas = sincronizador.obter_metricas()
    return {
        "config": config, "total_esperado": total_esperado,
        "leituras_gravadas": gravadas,
        "leituras_processadas": coletor.total,
        "tempo_geracao": tempo_geracao, "tempo_total": tempo_total,
        "taxa_geracao": gravadas / tempo_geracao if tempo_geracao else 0,
        "throughput": coletor.total / tempo_total if tempo_total else 0,
        "total_threads_sensores": total_threads_sensores,
        "total_threads_servidor": total_threads_servidor,
        "resultados": coletor.resultados, "estados": estados,
        "contador": contador.valor,
        "atualizacoes_perdidas": max(0, gravadas - contador.valor),
        "bloqueios": metricas["bloqueios"],
        "tempo_espera": metricas["tempo_espera"],
        "espera_processamento_media": (coletor.espera_total / coletor.total
                                        if coletor.total else 0.0),
        "esperas_prioridade": {p: soma / n if n else 0.0
                               for p, (soma, n) in coletor.esperas_prioridade.items()},
        "cancelado": stop_event.is_set(),
    }


class Aplicacao(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Simulador IoT — SQLite, Sincronização e Servidores")
        self.geometry("1250x800")
        self.minsize(1050, 680)
        self.stop_event = threading.Event()
        self.fila_ui = Queue()
        self.em_execucao = False
        self.variaveis = {}
        self._criar_interface()
        self.after(100, self._processar_fila_ui)

    def _criar_interface(self):
        principal = ttk.Frame(self, padding=10)
        principal.pack(fill="both", expand=True)
        principal.columnconfigure(1, weight=1)
        principal.rowconfigure(0, weight=1)
        painel = ttk.LabelFrame(principal, text="Parâmetros", padding=10)
        painel.grid(row=0, column=0, sticky="nsw", padx=(0, 10))
        campos = [
            ("num_sensores", "Número de sensores", "5"),
            ("threads_por_sensor", "Threads por sensor", "2"),
            ("leituras_por_sensor", "Leituras por sensor", "20"),
            ("intervalo_leituras", "Intervalo de leitura (s)", "0"),
            ("tempo_processamento", "Processamento (s)", "0.01"),
            ("num_servidores", "Número de servidores", "1"),
            ("threads_por_servidor", "Threads por servidor", "3"),
        ]
        linha = 0
        for chave, rotulo, padrao in campos:
            self.variaveis[chave] = tk.StringVar(value=padrao)
            ttk.Label(painel, text=rotulo).grid(row=linha, column=0, sticky="w", pady=4)
            ttk.Entry(painel, textvariable=self.variaveis[chave], width=14).grid(
                row=linha, column=1, sticky="ew", pady=4)
            linha += 1
        self.var_sincronizacao = tk.StringVar(value="mutex")
        ttk.Label(painel, text="Sincronização").grid(row=linha, column=0, sticky="w")
        ttk.Combobox(painel, textvariable=self.var_sincronizacao,
                     values=["nenhum", "mutex", "semaforo"],
                     state="readonly", width=12).grid(row=linha, column=1, sticky="ew")
        linha += 1
        self.var_capacidade = tk.StringVar(value="1")
        ttk.Label(painel, text="Capacidade do semáforo").grid(row=linha, column=0, sticky="w")
        ttk.Entry(painel, textvariable=self.var_capacidade, width=14).grid(
            row=linha, column=1, sticky="ew")
        linha += 1
        self.var_algoritmo = tk.StringVar(value="FCFS")
        ttk.Label(painel, text="Escalonamento").grid(row=linha, column=0, sticky="w")
        ttk.Combobox(painel, textvariable=self.var_algoritmo,
                     values=["FCFS", "Prioridade"], state="readonly",
                     width=14).grid(row=linha, column=1, sticky="ew")
        linha += 1
        self.var_grafico = tk.StringVar(value="Threads por servidor")
        ttk.Label(painel, text="Variável do gráfico").grid(row=linha, column=0, sticky="w", pady=8)
        ttk.Combobox(painel, textvariable=self.var_grafico,
                     values=["Threads por servidor", "Threads por sensor"],
                     state="readonly", width=21).grid(row=linha, column=1, sticky="ew")
        linha += 1
        ttk.Label(painel, text="No gráfico, o número de threads\n"
                              "é o máximo da varredura (1 até N).",
                  justify="left").grid(row=linha, column=0, columnspan=2, pady=8)
        linha += 1
        self.botao_executar = ttk.Button(painel, text="Executar configuração",
                                         command=self._executar_configuracao)
        self.botao_executar.grid(row=linha, column=0, columnspan=2, sticky="ew", pady=4)
        linha += 1
        self.botao_grafico = ttk.Button(painel, text="Gerar gráfico de linhas",
                                        command=self._gerar_grafico)
        self.botao_grafico.grid(row=linha, column=0, columnspan=2, sticky="ew", pady=4)
        linha += 1
        self.botao_comparar = ttk.Button(painel, text="Comparar FCFS x Prioridade",
                                         command=self._comparar_algoritmos)
        self.botao_comparar.grid(row=linha, column=0, columnspan=2, sticky="ew", pady=4)
        linha += 1
        self.botao_cancelar = ttk.Button(painel, text="Cancelar", state="disabled",
                                         command=self._cancelar)
        self.botao_cancelar.grid(row=linha, column=0, columnspan=2, sticky="ew", pady=4)
        linha += 1
        self.progresso = ttk.Progressbar(painel, mode="indeterminate")
        self.progresso.grid(row=linha, column=0, columnspan=2, sticky="ew", pady=8)
        linha += 1
        self.var_status = tk.StringVar(value="Pronto.")
        ttk.Label(painel, textvariable=self.var_status, wraplength=280).grid(
            row=linha, column=0, columnspan=2, sticky="w")

        direita = ttk.Frame(principal)
        direita.grid(row=0, column=1, sticky="nsew")
        direita.columnconfigure(0, weight=1)
        direita.rowconfigure(1, weight=1)
        frame_resultado = ttk.LabelFrame(direita, text="Resultado", padding=6)
        frame_resultado.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        self.tree_resultados = self._criar_tabela(
            frame_resultado, [("metrica", "Métrica", 270), ("valor", "Valor", 170)],
            altura=8)
        abas = ttk.Notebook(direita)
        abas.grid(row=1, column=0, sticky="nsew")
        self.figuras = []
        for nome, titulo, ylabel in [
            ("tempo", "Tempo total × número de threads", "Tempo total (s)"),
            ("throughput", "Throughput × número de threads", "Leituras/s"),
        ]:
            aba = ttk.Frame(abas)
            abas.add(aba, text="Gráfico" if nome == "tempo" else "Throughput")
            figura = Figure(figsize=(7, 4), dpi=100)
            eixo = figura.add_subplot(111)
            eixo.set_title(titulo)
            eixo.set_ylabel(ylabel)
            eixo.grid(True, alpha=0.3)
            canvas = FigureCanvasTkAgg(figura, master=aba)
            canvas.draw()
            canvas.get_tk_widget().pack(fill="both", expand=True)
            self.figuras.append((eixo, figura, canvas))
        aba_curva = ttk.Frame(abas)
        abas.add(aba_curva, text="Tabela da curva")
        self.tree_curva = self._criar_tabela(aba_curva, [
            ("valor", "Threads", 80), ("workers", "Workers servidor", 120),
            ("tempo", "Tempo (s)", 110), ("throughput", "Leituras/s", 110),
            ("processadas", "Concluídas", 110)])
        aba_leituras = ttk.Frame(abas)
        abas.add(aba_leituras, text="Últimas leituras")
        self.tree_leituras = self._criar_tabela(aba_leituras, [
            ("sensor", "Sensor", 65), ("temperatura", "Temperatura", 100),
            ("horario", "Horário", 135), ("prioridade", "Prioridade", 80),
            ("servidor", "Servidor", 75), ("thread", "Thread", 190),
            ("status", "Status", 100), ("espera", "Espera fila (s)", 110)])

        # Painel de estados ao vivo (atualizado pelo monitor durante a execução).
        frame_estados = ttk.LabelFrame(direita, text="Estados ao vivo", padding=6)
        frame_estados.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        self.var_estados = {}
        for coluna, (chave, rotulo) in enumerate([
                ("pendente", "Pendentes"), ("na_fila", "Na fila"),
                ("processando", "Processando"), ("concluido", "Concluídas")]):
            self.var_estados[chave] = tk.StringVar(value="0")
            ttk.Label(frame_estados, text=f"{rotulo}:").grid(
                row=0, column=coluna * 2, padx=(8, 2))
            ttk.Label(frame_estados, textvariable=self.var_estados[chave],
                      font=("TkDefaultFont", 11, "bold")).grid(
                row=0, column=coluna * 2 + 1, padx=(0, 12))

    @staticmethod
    def _criar_tabela(parent, colunas, altura=14):
        tree = ttk.Treeview(parent, columns=[c[0] for c in colunas],
                            show="headings", height=altura)
        for chave, titulo, largura in colunas:
            tree.heading(chave, text=titulo)
            tree.column(chave, width=largura, anchor="center")
        tree.pack(fill="both", expand=True)
        return tree

    def _ler_configuracao(self):
        try:
            v = self.variaveis
            config = Configuracao(
                num_sensores=int(v["num_sensores"].get()),
                threads_por_sensor=int(v["threads_por_sensor"].get()),
                leituras_por_sensor=int(v["leituras_por_sensor"].get()),
                intervalo_leituras=float(v["intervalo_leituras"].get()),
                tempo_processamento=float(v["tempo_processamento"].get()),
                num_servidores=int(v["num_servidores"].get()),
                threads_por_servidor=int(v["threads_por_servidor"].get()),
                modo_sincronizador=self.var_sincronizacao.get(),
                capacidade_semaforo=int(self.var_capacidade.get()),
                algoritmo=self.var_algoritmo.get())
        except ValueError as erro:
            raise ValueError("Preencha os campos com números válidos.") from erro
        for campo in ("num_sensores", "threads_por_sensor", "leituras_por_sensor",
                      "num_servidores", "threads_por_servidor", "capacidade_semaforo"):
            if getattr(config, campo) < 1:
                raise ValueError(f"{campo} deve ser >= 1")
        if config.intervalo_leituras < 0 or config.tempo_processamento < 0:
            raise ValueError("Tempos não podem ser negativos")
        return config

    def _set_em_execucao(self, ativo):
        self.em_execucao = ativo
        self.botao_executar.configure(state="disabled" if ativo else "normal")
        self.botao_grafico.configure(state="disabled" if ativo else "normal")
        self.botao_comparar.configure(state="disabled" if ativo else "normal")
        self.botao_cancelar.configure(state="normal" if ativo else "disabled")
        if ativo:
            self.progresso.start(10)
        else:
            self.progresso.stop()

    def _iniciar(self, alvo, args):
        if self.em_execucao:
            return
        self.stop_event = threading.Event()
        self._set_em_execucao(True)
        threading.Thread(target=alvo, args=args, daemon=True).start()

    def _executar_configuracao(self):
        try:
            config = self._ler_configuracao()
        except ValueError as erro:
            messagebox.showerror("Configuração inválida", str(erro))
            return
        self.var_status.set("Executando configuração...")
        self._iniciar(self._worker_configuracao, (config,))

    def _worker_configuracao(self, config):
        try:
            resultado = executar_simulacao(
                config, self.stop_event, True,
                callback_estados=lambda e: self.fila_ui.put(("estados", e)))
            self.fila_ui.put(("resultado", resultado))
        except Exception as erro:
            self.fila_ui.put(("erro", str(erro)))

    def _gerar_grafico(self):
        try:
            config = self._ler_configuracao()
        except ValueError as erro:
            messagebox.showerror("Configuração inválida", str(erro))
            return
        variavel = self.var_grafico.get()
        maximo = (config.threads_por_servidor if variavel == "Threads por servidor"
                  else config.threads_por_sensor)
        self._limpar_tabela(self.tree_curva)
        self.var_status.set(f"Gerando curva: {variavel}, de 1 até {maximo}...")
        self._iniciar(self._worker_grafico, (config, variavel, maximo))

    def _worker_grafico(self, config, variavel, maximo):
        pontos = []
        try:
            for valor in range(1, maximo + 1):
                if self.stop_event.is_set():
                    break
                config_teste = replace(config, **{
                    "threads_por_servidor" if variavel == "Threads por servidor"
                    else "threads_por_sensor": valor})
                self.fila_ui.put(("status", f"Teste {valor}/{maximo}"))
                r = executar_simulacao(config_teste, self.stop_event, False)
                ponto = {"valor": valor, "tempo_total": r["tempo_total"],
                         "throughput": r["throughput"],
                         "leituras_processadas": r["leituras_processadas"],
                         "threads_servidor_totais": r["total_threads_servidor"]}
                pontos.append(ponto)
                self.fila_ui.put(("ponto", ponto))
            self.fila_ui.put(("grafico", (variavel, pontos, self.stop_event.is_set())))
        except Exception as erro:
            self.fila_ui.put(("erro", str(erro)))

    def _comparar_algoritmos(self):
        try:
            config = self._ler_configuracao()
        except ValueError as erro:
            messagebox.showerror("Configuração inválida", str(erro))
            return
        # Um servidor/uma thread simulam a escolha da próxima tarefa por uma CPU.
        config = replace(config, num_servidores=1, threads_por_servidor=1)
        self.var_status.set("Comparando FCFS e Prioridade (1 worker)...")
        self._iniciar(self._worker_comparacao, (config,))

    def _worker_comparacao(self, config):
        try:
            # Mesmas temperaturas e prioridades nas duas rodadas.
            carga = {}
            for sensor_id in range(config.num_sensores):
                sensor = IotSensor(sensor_id, None, None, None)
                carga[sensor_id] = [sensor.gera_medida()
                                    for _ in range(config.leituras_por_sensor)]
            resultados = []
            for algoritmo in ("FCFS", "Prioridade"):
                if self.stop_event.is_set():
                    break
                self.fila_ui.put(("status", f"Testando {algoritmo}..."))
                resultado = executar_simulacao(replace(config, algoritmo=algoritmo),
                                              self.stop_event, False, carga=carga)
                resultados.append(resultado)
            self.fila_ui.put(("comparacao", resultados))
        except Exception as erro:
            self.fila_ui.put(("erro", str(erro)))

    def _cancelar(self):
        self.stop_event.set()
        self.var_status.set("Cancelamento solicitado...")

    def _processar_fila_ui(self):
        try:
            while True:
                tipo, payload = self.fila_ui.get_nowait()
                if tipo == "resultado":
                    self._mostrar_resultado(payload)
                    self._set_em_execucao(False)
                    self.var_status.set("Cancelada." if payload["cancelado"]
                                        else "Configuração concluída.")
                elif tipo == "ponto":
                    self.tree_curva.insert("", "end", values=(
                        payload["valor"], payload["threads_servidor_totais"],
                        f'{payload["tempo_total"]:.4f}',
                        f'{payload["throughput"]:.2f}',
                        payload["leituras_processadas"]))
                elif tipo == "grafico":
                    variavel, pontos, cancelado = payload
                    self._mostrar_grafico(variavel, pontos)
                    self._set_em_execucao(False)
                    self.var_status.set("Curva interrompida." if cancelado
                                        else "Gráfico concluído.")
                elif tipo == "comparacao":
                    self._mostrar_comparacao(payload)
                    self._set_em_execucao(False)
                    self.var_status.set("Comparação concluída." if len(payload) == 2
                                        and not self.stop_event.is_set() else "Interrompida.")
                elif tipo == "estados":
                    self._atualizar_estados(payload)
                elif tipo == "status":
                    self.var_status.set(payload)
                elif tipo == "erro":
                    self._set_em_execucao(False)
                    self.var_status.set("Erro na execução.")
                    messagebox.showerror("Erro", payload)
                self.fila_ui.task_done()
        except Empty:
            pass
        self.after(100, self._processar_fila_ui)

    def _atualizar_estados(self, estados):
        for chave, var in self.var_estados.items():
            var.set(str(estados.get(chave, 0)))

    def _mostrar_resultado(self, r):
        self._limpar_tabela(self.tree_resultados)
        itens = [
            ("Sensores", r["config"].num_sensores),
            ("Modo de sincronização", r["config"].modo_sincronizador),
            ("Algoritmo", r["config"].algoritmo),
            ("Espera média na fila", f'{r["espera_processamento_media"]:.4f} s'),
            ("Threads de sensores", r["total_threads_sensores"]),
            ("Threads de servidores", r["total_threads_servidor"]),
            ("Leituras esperadas", r["total_esperado"]),
            ("Leituras gravadas", r["leituras_gravadas"]),
            ("Pendentes", r["estados"].get("pendente", 0)),
            ("Na fila", r["estados"].get("na_fila", 0)),
            ("Processando", r["estados"].get("processando", 0)),
            ("Concluídas", r["estados"].get("concluido", 0)),
            ("Contador demonstrativo", r["contador"]),
            ("Atualizações perdidas", r["atualizacoes_perdidas"]),
            ("Bloqueios", r["bloqueios"]),
            ("Tempo total de espera", f'{r["tempo_espera"]:.4f} s'),
            ("Tempo de geração", f'{r["tempo_geracao"]:.4f} s'),
            ("Tempo total", f'{r["tempo_total"]:.4f} s'),
            ("Taxa de geração", f'{r["taxa_geracao"]:.2f} leituras/s'),
            ("Throughput", f'{r["throughput"]:.2f} leituras/s'),
        ]
        for item in itens:
            self.tree_resultados.insert("", "end", values=item)
        self._limpar_tabela(self.tree_leituras)
        for leitura in r["resultados"][-100:]:
            horario = datetime.fromtimestamp(leitura["horario"]).strftime(
                "%H:%M:%S.%f")[:-3]
            self.tree_leituras.insert("", "end", values=(
                leitura["sensor_id"], f'{leitura["temperatura"]:.2f} °C',
                horario, leitura["prioridade"], leitura["servidor_id"],
                leitura["thread_servidor"], leitura["status"],
                f'{leitura["espera_processamento"]:.4f}'))

    def _mostrar_grafico(self, variavel, pontos):
        for indice, chave in enumerate(("tempo_total", "throughput")):
            eixo, figura, canvas = self.figuras[indice]
            eixo.clear()
            if pontos:
                x = [p["valor"] for p in pontos]
                eixo.plot(x, [p[chave] for p in pontos], marker="o", linewidth=2)
                eixo.set_xticks(x)
            eixo.set_title(("Tempo total" if indice == 0 else "Throughput")
                           + " × " + variavel)
            eixo.set_xlabel(variavel)
            eixo.set_ylabel("Segundos" if indice == 0 else "Leituras/s")
            eixo.grid(True, alpha=0.3)
            figura.tight_layout()
            canvas.draw()

    def _mostrar_comparacao(self, resultados):
        if not resultados:
            return
        nomes = [r["config"].algoritmo for r in resultados]
        espera = [r["espera_processamento_media"] for r in resultados]
        prioridade1 = [r["esperas_prioridade"][1] for r in resultados]
        eixo, figura, canvas = self.figuras[0]
        eixo.clear()
        x = range(len(resultados))
        eixo.bar([i - 0.17 for i in x], espera, width=0.34, label="Média geral")
        eixo.bar([i + 0.17 for i in x], prioridade1, width=0.34,
                 label="Prioridade 1")
        eixo.set_xticks(list(x), nomes)
        eixo.set_ylabel("Espera na fila (s)")
        eixo.set_title("Comparação: FCFS x Prioridade")
        eixo.legend()
        figura.tight_layout()
        canvas.draw()
        eixo, figura, canvas = self.figuras[1]
        eixo.clear()
        eixo.bar(nomes, [r["throughput"] for r in resultados])
        eixo.set_ylabel("Leituras/s")
        eixo.set_title("Vazão: FCFS x Prioridade")
        figura.tight_layout()
        canvas.draw()

    @staticmethod
    def _limpar_tabela(tree):
        for item in tree.get_children():
            tree.delete(item)


if __name__ == "__main__":
    Aplicacao().mainloop()

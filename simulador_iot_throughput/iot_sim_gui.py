import random
import time
import threading
from queue import Queue, Empty
from dataclasses import dataclass, replace
from datetime import datetime

import tkinter as tk
from tkinter import ttk, messagebox

from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

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


class Database:
    def __init__(self):
        self.conexao = sqlite3.connect('leituras_sensores.db')
        self.conexao.execute("""
        CREATE TABLE IF NOT EXISTS leituras (
        leitura_id INTEGER PRIMARY KEY AUTOINCREMENT,
        sensor_id INTEGER NOT NULL,
        temperatura REAL NOT NULL,
        horario REAL NOT NULL,
        status TEXT NOT NULL,
        prioridade INTEGER NOT NULL)
        """)
        self.conexao.commit()


    def gravacao(self, leitura):
        self.conexao.execute("""
        INSERT INTO leituras(
        sensor_id,
        temperatura, 
        horario,
        status,
        prioridade)
        VALUES (?, ?, ?, ?, ?)
        """, (leitura["sensor_id"],
        leitura["temperatura"],
        leitura["horario"],
        "pendente",
        leitura["prioridade"]))

        self.conexao.commit()


class Sincronizador:
    def __init__(self, modo="mutex", n_semaf = 1):
        self.modo = modo

        if modo == "mutex":
            self.recurso = threading.Lock()

        elif modo == "semaforo":
            self.recurso = threading.Semaphore(n_semaf)

        elif modo == "nenhum":
            self.recurso = None

        else:
            raise ValueError("Modo de sincronização inválido.")

    def adquirir(self):
        if self.recurso is not None:
            self.recurso.acquire()

    def liberar(self):
        if self.recurso is not None:
            self.recurso.release()


class IotSensor:
    def __init__(self, sensor_id):
        self.sensor_id = sensor_id

    def gera_medida(self):
        return {
            "sensor_id": self.sensor_id,
            "temperatura": random.uniform(15, 35),
            "horario": time.time(),
        }

    def leitura(self, fila, quantidade, intervalo, stop_event):
        for indice in range(quantidade):
            if stop_event.is_set():
                break

            fila.put(self.gera_medida())

            if stop_event.wait(intervalo):
                break


class ColetorResultados:
    def __init__(self, guardar_leituras=True):
        self.guardar_leituras = guardar_leituras
        self.resultados = []
        self.total = 0
        self.lock = threading.Lock()

    def adicionar(self, leitura):
        with self.lock:
            self.total += 1

            if self.guardar_leituras:
                self.resultados.append(leitura)


class Servidor:
    def __init__(
        self,
        servidor_id,
        fila,
        tempo_processamento,
        coletor,
        stop_event,
    ):
        self.servidor_id = servidor_id
        self.fila = fila
        self.tempo_processamento = tempo_processamento
        self.coletor = coletor
        self.stop_event = stop_event

    def run(self):
        while True:
            leitura = self.fila.get()

            try:
                if leitura is SENTINELA:
                    return

                cancelado = self.stop_event.wait(self.tempo_processamento)

                if cancelado:
                    continue

                leitura_processada = dict(leitura)
                leitura_processada["servidor_id"] = self.servidor_id
                leitura_processada["thread_servidor"] = threading.current_thread().name

                self.coletor.adicionar(leitura_processada)

            finally:
                self.fila.task_done()


def dividir_trabalho(total, quantidade_threads):
    base = total // quantidade_threads
    resto = total % quantidade_threads

    return [
        base + (1 if indice < resto else 0)
        for indice in range(quantidade_threads)
    ]


def executar_simulacao(config, stop_event=None, guardar_leituras=True):
    if stop_event is None:
        stop_event = threading.Event()

    fila = Queue()
    coletor = ColetorResultados(guardar_leituras=guardar_leituras)

    total_esperado = config.num_sensores * config.leituras_por_sensor
    total_threads_sensores = config.num_sensores * config.threads_por_sensor
    total_threads_servidor = config.num_servidores * config.threads_por_servidor

    servidores = []
    threads_servidores = []

    for servidor_id in range(config.num_servidores):
        servidor = Servidor(
            servidor_id=servidor_id,
            fila=fila,
            tempo_processamento=config.tempo_processamento,
            coletor=coletor,
            stop_event=stop_event,
        )
        servidores.append(servidor)

        for thread_id in range(config.threads_por_servidor):
            thread = threading.Thread(
                target=servidor.run,
                name=f"Servidor-{servidor_id}-Thread-{thread_id}",
                daemon=True,
            )
            threads_servidores.append(thread)

    sensores = [IotSensor(i) for i in range(config.num_sensores)]
    threads_sensores = []

    for sensor in sensores:
        distribuicao = dividir_trabalho(
            config.leituras_por_sensor,
            config.threads_por_sensor,
        )

        for thread_id, quantidade in enumerate(distribuicao):
            thread = threading.Thread(
                target=sensor.leitura,
                args=(
                    fila,
                    quantidade,
                    config.intervalo_leituras,
                    stop_event,
                ),
                name=f"Sensor-{sensor.sensor_id}-Thread-{thread_id}",
                daemon=True,
            )
            threads_sensores.append(thread)

    inicio = time.perf_counter()

    for thread in threads_servidores:
        thread.start()

    for thread in threads_sensores:
        thread.start()

    for thread in threads_sensores:
        thread.join()

    fim_sensores = time.perf_counter()
    tempo_geracao = fim_sensores - inicio

    for _ in range(total_threads_servidor):
        fila.put(SENTINELA)

    for thread in threads_servidores:
        thread.join()

    fim = time.perf_counter()
    tempo_total = fim - inicio

    leituras_processadas = coletor.total
    throughput = (
        leituras_processadas / tempo_total
        if tempo_total > 0
        else 0.0
    )

    taxa_geracao = (
        total_esperado / tempo_geracao
        if tempo_geracao > 0
        else 0.0
    )

    return {
        "config": config,
        "total_esperado": total_esperado,
        "leituras_processadas": leituras_processadas,
        "tempo_geracao": tempo_geracao,
        "tempo_total": tempo_total,
        "throughput": throughput,
        "taxa_geracao": taxa_geracao,
        "total_threads_sensores": total_threads_sensores,
        "total_threads_servidor": total_threads_servidor,
        "resultados": coletor.resultados,
        "cancelado": stop_event.is_set(),
    }


class Aplicacao(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title("Simulador IoT - Sensores e Servidores Multithread")
        self.geometry("1280x820")
        self.minsize(1080, 700)

        self.stop_event = threading.Event()
        self.fila_ui = Queue()
        self.em_execucao = False

        self._criar_variaveis()
        self._criar_interface()

        self.after(100, self._processar_fila_ui)

    def _criar_variaveis(self):
        self.var_num_sensores = tk.StringVar(value="5")
        self.var_threads_sensor = tk.StringVar(value="1")
        self.var_num_leituras = tk.StringVar(value="10")
        self.var_intervalo = tk.StringVar(value="1.0")
        self.var_tempo_processamento = tk.StringVar(value="0.5")
        self.var_num_servidores = tk.StringVar(value="1")
        self.var_threads_servidor = tk.StringVar(value="5")

        self.var_grafico = tk.StringVar(value="Threads por servidor")
        self.var_status = tk.StringVar(value="Pronto.")

    def _criar_interface(self):
        principal = ttk.Frame(self, padding=10)
        principal.pack(fill="both", expand=True)

        principal.columnconfigure(0, weight=0)
        principal.columnconfigure(1, weight=1)
        principal.rowconfigure(0, weight=1)

        painel = ttk.LabelFrame(
            principal,
            text="Parametros da simulacao",
            padding=12,
        )
        painel.grid(row=0, column=0, sticky="nsw", padx=(0, 10))

        linha = 0

        linha = self._campo(
            painel,
            linha,
            "Numero de sensores",
            self.var_num_sensores,
        )

        linha = self._campo(
            painel,
            linha,
            "Threads por sensor",
            self.var_threads_sensor,
        )

        linha = self._campo(
            painel,
            linha,
            "Leituras por sensor",
            self.var_num_leituras,
        )

        linha = self._campo(
            painel,
            linha,
            "Intervalo entre leituras (s)",
            self.var_intervalo,
        )

        linha = self._campo(
            painel,
            linha,
            "Tempo processamento/leitura (s)",
            self.var_tempo_processamento,
        )

        linha = self._campo(
            painel,
            linha,
            "Numero de servidores",
            self.var_num_servidores,
        )

        linha = self._campo(
            painel,
            linha,
            "Threads por servidor",
            self.var_threads_servidor,
        )

        ttk.Separator(painel).grid(
            row=linha,
            column=0,
            columnspan=2,
            sticky="ew",
            pady=12,
        )
        linha += 1

        ttk.Label(
            painel,
            text="Variavel do grafico",
        ).grid(
            row=linha,
            column=0,
            sticky="w",
            pady=4,
        )

        combo = ttk.Combobox(
            painel,
            textvariable=self.var_grafico,
            state="readonly",
            width=22,
            values=[
                "Threads por servidor",
                "Threads por sensor",
            ],
        )
        combo.grid(
            row=linha,
            column=1,
            sticky="ew",
            pady=4,
        )
        linha += 1

        texto_ajuda = (
            "No grafico, o valor informado no campo de threads\n"
            "vira o MAXIMO da varredura.\n\n"
            "Exemplo: Threads por servidor = 5\n"
            "Executa: 1, 2, 3, 4 e 5 threads por servidor."
        )

        ttk.Label(
            painel,
            text=texto_ajuda,
            justify="left",
            foreground="#555555",
        ).grid(
            row=linha,
            column=0,
            columnspan=2,
            sticky="w",
            pady=(4, 12),
        )
        linha += 1

        self.botao_executar = ttk.Button(
            painel,
            text="Executar configuracao",
            command=self._executar_configuracao,
        )
        self.botao_executar.grid(
            row=linha,
            column=0,
            columnspan=2,
            sticky="ew",
            pady=4,
        )
        linha += 1

        self.botao_grafico = ttk.Button(
            painel,
            text="Gerar grafico de linhas",
            command=self._gerar_grafico,
        )
        self.botao_grafico.grid(
            row=linha,
            column=0,
            columnspan=2,
            sticky="ew",
            pady=4,
        )
        linha += 1

        self.botao_cancelar = ttk.Button(
            painel,
            text="Cancelar",
            command=self._cancelar,
            state="disabled",
        )
        self.botao_cancelar.grid(
            row=linha,
            column=0,
            columnspan=2,
            sticky="ew",
            pady=4,
        )
        linha += 1

        self.progresso = ttk.Progressbar(
            painel,
            mode="indeterminate",
        )
        self.progresso.grid(
            row=linha,
            column=0,
            columnspan=2,
            sticky="ew",
            pady=(12, 4),
        )
        linha += 1

        ttk.Label(
            painel,
            textvariable=self.var_status,
            wraplength=300,
        ).grid(
            row=linha,
            column=0,
            columnspan=2,
            sticky="w",
            pady=4,
        )

        direita = ttk.Frame(principal)
        direita.grid(row=0, column=1, sticky="nsew")
        direita.rowconfigure(1, weight=1)
        direita.columnconfigure(0, weight=1)

        resultados_frame = ttk.LabelFrame(
            direita,
            text="Resultado da configuracao",
            padding=8,
        )
        resultados_frame.grid(
            row=0,
            column=0,
            sticky="ew",
            pady=(0, 10),
        )

        self.tree_resultados = ttk.Treeview(
            resultados_frame,
            columns=("metrica", "valor"),
            show="headings",
            height=7,
        )
        self.tree_resultados.heading("metrica", text="Metrica")
        self.tree_resultados.heading("valor", text="Valor")
        self.tree_resultados.column("metrica", width=300)
        self.tree_resultados.column("valor", width=220)
        self.tree_resultados.pack(fill="x", expand=True)

        notebook = ttk.Notebook(direita)
        notebook.grid(row=1, column=0, sticky="nsew")

        aba_grafico = ttk.Frame(notebook)
        notebook.add(aba_grafico, text="Grafico")

        self.figura = Figure(figsize=(8, 5), dpi=100)
        self.eixo = self.figura.add_subplot(111)

        self.eixo.set_title("Tempo total x numero de threads")
        self.eixo.set_xlabel("Threads")
        self.eixo.set_ylabel("Tempo total (s)")
        self.eixo.grid(True, alpha=0.3)

        self.canvas = FigureCanvasTkAgg(
            self.figura,
            master=aba_grafico,
        )
        self.canvas.draw()
        self.canvas.get_tk_widget().pack(
            fill="both",
            expand=True,
        )

        aba_throughput = ttk.Frame(notebook)
        notebook.add(aba_throughput, text="Throughput")

        self.figura_throughput = Figure(figsize=(8, 5), dpi=100)
        self.eixo_throughput = self.figura_throughput.add_subplot(111)

        self.eixo_throughput.set_title("Throughput x numero de threads")
        self.eixo_throughput.set_xlabel("Threads")
        self.eixo_throughput.set_ylabel("Throughput (leituras/s)")
        self.eixo_throughput.grid(True, alpha=0.3)

        self.canvas_throughput = FigureCanvasTkAgg(
            self.figura_throughput,
            master=aba_throughput,
        )
        self.canvas_throughput.draw()
        self.canvas_throughput.get_tk_widget().pack(
            fill="both",
            expand=True,
        )

        aba_curva = ttk.Frame(notebook)
        notebook.add(aba_curva, text="Tabela da curva")

        colunas_curva = (
            "valor",
            "workers",
            "tempo",
            "throughput",
            "processadas",
        )

        self.tree_curva = ttk.Treeview(
            aba_curva,
            columns=colunas_curva,
            show="headings",
        )

        self.tree_curva.heading("valor", text="Valor variado")
        self.tree_curva.heading("workers", text="Threads servidor totais")
        self.tree_curva.heading("tempo", text="Tempo total (s)")
        self.tree_curva.heading("throughput", text="Throughput (leituras/s)")
        self.tree_curva.heading("processadas", text="Leituras processadas")

        self.tree_curva.column("valor", width=110, anchor="center")
        self.tree_curva.column("workers", width=160, anchor="center")
        self.tree_curva.column("tempo", width=130, anchor="center")
        self.tree_curva.column("throughput", width=170, anchor="center")
        self.tree_curva.column("processadas", width=160, anchor="center")

        self.tree_curva.pack(fill="both", expand=True)

        aba_leituras = ttk.Frame(notebook)
        notebook.add(aba_leituras, text="Ultimas leituras")

        colunas_leituras = (
            "sensor",
            "temperatura",
            "horario",
            "servidor",
            "thread",
        )

        self.tree_leituras = ttk.Treeview(
            aba_leituras,
            columns=colunas_leituras,
            show="headings",
        )

        self.tree_leituras.heading("sensor", text="Sensor")
        self.tree_leituras.heading("temperatura", text="Temperatura")
        self.tree_leituras.heading("horario", text="Horario")
        self.tree_leituras.heading("servidor", text="Servidor")
        self.tree_leituras.heading("thread", text="Thread servidor")

        self.tree_leituras.column("sensor", width=80, anchor="center")
        self.tree_leituras.column("temperatura", width=120, anchor="center")
        self.tree_leituras.column("horario", width=170, anchor="center")
        self.tree_leituras.column("servidor", width=90, anchor="center")
        self.tree_leituras.column("thread", width=220)

        self.tree_leituras.pack(fill="both", expand=True)

    def _campo(self, parent, linha, texto, variavel):
        ttk.Label(
            parent,
            text=texto,
        ).grid(
            row=linha,
            column=0,
            sticky="w",
            pady=4,
            padx=(0, 8),
        )

        entrada = ttk.Entry(
            parent,
            textvariable=variavel,
            width=12,
        )
        entrada.grid(
            row=linha,
            column=1,
            sticky="ew",
            pady=4,
        )

        return linha + 1

    def _ler_configuracao(self):
        try:
            config = Configuracao(
                num_sensores=int(self.var_num_sensores.get()),
                threads_por_sensor=int(self.var_threads_sensor.get()),
                leituras_por_sensor=int(self.var_num_leituras.get()),
                intervalo_leituras=float(self.var_intervalo.get()),
                tempo_processamento=float(
                    self.var_tempo_processamento.get()
                ),
                num_servidores=int(self.var_num_servidores.get()),
                threads_por_servidor=int(
                    self.var_threads_servidor.get()
                ),
            )
        except ValueError:
            raise ValueError(
                "Preencha os campos numericos com valores validos."
            )

        inteiros_positivos = {
            "Numero de sensores": config.num_sensores,
            "Threads por sensor": config.threads_por_sensor,
            "Leituras por sensor": config.leituras_por_sensor,
            "Numero de servidores": config.num_servidores,
            "Threads por servidor": config.threads_por_servidor,
        }

        for nome, valor in inteiros_positivos.items():
            if valor < 1:
                raise ValueError(f"{nome} deve ser maior ou igual a 1.")

        if config.intervalo_leituras < 0:
            raise ValueError(
                "Intervalo entre leituras nao pode ser negativo."
            )

        if config.tempo_processamento < 0:
            raise ValueError(
                "Tempo de processamento nao pode ser negativo."
            )

        return config

    def _set_em_execucao(self, ativo):
        self.em_execucao = ativo

        if ativo:
            self.botao_executar.configure(state="disabled")
            self.botao_grafico.configure(state="disabled")
            self.botao_cancelar.configure(state="normal")
            self.progresso.start(10)
        else:
            self.botao_executar.configure(state="normal")
            self.botao_grafico.configure(state="normal")
            self.botao_cancelar.configure(state="disabled")
            self.progresso.stop()

    def _executar_configuracao(self):
        if self.em_execucao:
            return

        try:
            config = self._ler_configuracao()
        except ValueError as erro:
            messagebox.showerror("Configuracao invalida", str(erro))
            return

        self.stop_event = threading.Event()
        self._set_em_execucao(True)
        self.var_status.set("Executando configuracao...")

        thread = threading.Thread(
            target=self._worker_configuracao,
            args=(config,),
            daemon=True,
        )
        thread.start()

    def _worker_configuracao(self, config):
        try:
            resultado = executar_simulacao(
                config,
                stop_event=self.stop_event,
                guardar_leituras=True,
            )

            self.fila_ui.put(("resultado", resultado))

        except Exception as erro:
            self.fila_ui.put(("erro", str(erro)))

    def _gerar_grafico(self):
        if self.em_execucao:
            return

        try:
            config = self._ler_configuracao()
        except ValueError as erro:
            messagebox.showerror("Configuracao invalida", str(erro))
            return

        variavel = self.var_grafico.get()

        if variavel == "Threads por servidor":
            maximo = config.threads_por_servidor
        else:
            maximo = config.threads_por_sensor

        self.stop_event = threading.Event()
        self._set_em_execucao(True)

        self.var_status.set(
            f"Gerando curva: {variavel}, de 1 ate {maximo}..."
        )

        self._limpar_tree(self.tree_curva)

        thread = threading.Thread(
            target=self._worker_grafico,
            args=(config, variavel, maximo),
            daemon=True,
        )
        thread.start()

    def _worker_grafico(self, config, variavel, maximo):
        pontos = []

        try:
            for valor in range(1, maximo + 1):
                if self.stop_event.is_set():
                    break

                if variavel == "Threads por servidor":
                    config_teste = replace(
                        config,
                        threads_por_servidor=valor,
                    )
                else:
                    config_teste = replace(
                        config,
                        threads_por_sensor=valor,
                    )

                self.fila_ui.put(
                    (
                        "status",
                        f"Teste {valor}/{maximo}: {variavel} = {valor}",
                    )
                )

                resultado = executar_simulacao(
                    config_teste,
                    stop_event=self.stop_event,
                    guardar_leituras=False,
                )

                pontos.append(
                    {
                        "valor": valor,
                        "tempo_total": resultado["tempo_total"],
                        "throughput": resultado["throughput"],
                        "leituras_processadas": resultado[
                            "leituras_processadas"
                        ],
                        "threads_servidor_totais": resultado[
                            "total_threads_servidor"
                        ],
                    }
                )

                self.fila_ui.put(("ponto_curva", pontos[-1]))

            self.fila_ui.put(
                (
                    "grafico_pronto",
                    {
                        "variavel": variavel,
                        "pontos": pontos,
                        "cancelado": self.stop_event.is_set(),
                    },
                )
            )

        except Exception as erro:
            self.fila_ui.put(("erro", str(erro)))

    def _cancelar(self):
        if self.em_execucao:
            self.stop_event.set()
            self.var_status.set("Cancelamento solicitado...")

    def _processar_fila_ui(self):
        try:
            while True:
                tipo, payload = self.fila_ui.get_nowait()

                if tipo == "resultado":
                    self._mostrar_resultado(payload)
                    self._set_em_execucao(False)

                    if payload["cancelado"]:
                        self.var_status.set("Execucao cancelada.")
                    else:
                        self.var_status.set("Configuracao concluida.")

                elif tipo == "status":
                    self.var_status.set(payload)

                elif tipo == "ponto_curva":
                    self._adicionar_ponto_tabela(payload)

                elif tipo == "grafico_pronto":
                    self._mostrar_grafico(
                        payload["variavel"],
                        payload["pontos"],
                    )
                    self._set_em_execucao(False)

                    if payload["cancelado"]:
                        self.var_status.set(
                            "Curva interrompida pelo usuario."
                        )
                    else:
                        self.var_status.set(
                            "Grafico de linhas concluido."
                        )

                elif tipo == "erro":
                    self._set_em_execucao(False)
                    self.var_status.set("Erro.")
                    messagebox.showerror("Erro", payload)

                self.fila_ui.task_done()

        except Empty:
            pass

        self.after(100, self._processar_fila_ui)

    def _mostrar_resultado(self, resultado):
        self._limpar_tree(self.tree_resultados)

        config = resultado["config"]

        metricas = [
            ("Sensores", config.num_sensores),
            (
                "Threads de sensores totais",
                resultado["total_threads_sensores"],
            ),
            (
                "Threads de servidor totais",
                resultado["total_threads_servidor"],
            ),
            ("Leituras esperadas", resultado["total_esperado"]),
            (
                "Leituras processadas",
                resultado["leituras_processadas"],
            ),
            (
                "Tempo de geracao dos sensores",
                f'{resultado["tempo_geracao"]:.4f} s',
            ),
            (
                "Tempo total do sistema",
                f'{resultado["tempo_total"]:.4f} s',
            ),
            (
                "Taxa de geracao",
                f'{resultado["taxa_geracao"]:.4f} leituras/s',
            ),
            (
                "Throughput",
                f'{resultado["throughput"]:.4f} leituras/s',
            ),
        ]

        for metrica, valor in metricas:
            self.tree_resultados.insert(
                "",
                "end",
                values=(metrica, valor),
            )

        self._limpar_tree(self.tree_leituras)

        ultimas = resultado["resultados"][-100:]

        for leitura in ultimas:
            horario = datetime.fromtimestamp(
                leitura["horario"]
            ).strftime("%H:%M:%S.%f")[:-3]

            self.tree_leituras.insert(
                "",
                "end",
                values=(
                    leitura["sensor_id"],
                    f'{leitura["temperatura"]:.2f} C',
                    horario,
                    leitura.get("servidor_id", "-"),
                    leitura.get("thread_servidor", "-"),
                ),
            )

    def _adicionar_ponto_tabela(self, ponto):
        self.tree_curva.insert(
            "",
            "end",
            values=(
                ponto["valor"],
                ponto["threads_servidor_totais"],
                f'{ponto["tempo_total"]:.4f}',
                f'{ponto["throughput"]:.4f}',
                ponto["leituras_processadas"],
            ),
        )

    def _mostrar_grafico(self, variavel, pontos):
        self.eixo.clear()

        if not pontos:
            self.eixo.set_title("Nenhum ponto gerado")
            self.canvas.draw()

            self.eixo_throughput.clear()
            self.eixo_throughput.set_title("Nenhum ponto gerado")
            self.canvas_throughput.draw()
            return

        x = [p["valor"] for p in pontos]
        y = [p["tempo_total"] for p in pontos]

        self.eixo.plot(
            x,
            y,
            marker="o",
            linewidth=2,
        )

        self.eixo.set_title(
            f"Tempo total x {variavel}"
        )
        self.eixo.set_xlabel(variavel)
        self.eixo.set_ylabel("Tempo total do sistema (s)")
        self.eixo.set_xticks(x)
        self.eixo.grid(True, alpha=0.3)

        self.figura.tight_layout()
        self.canvas.draw()

        throughput = [p["throughput"] for p in pontos]

        self.eixo_throughput.clear()
        self.eixo_throughput.plot(
            x,
            throughput,
            marker="o",
            linewidth=2,
        )
        self.eixo_throughput.set_title(
            f"Throughput x {variavel}"
        )
        self.eixo_throughput.set_xlabel(variavel)
        self.eixo_throughput.set_ylabel("Throughput (leituras/s)")
        self.eixo_throughput.set_xticks(x)
        self.eixo_throughput.grid(True, alpha=0.3)

        self.figura_throughput.tight_layout()
        self.canvas_throughput.draw()

    @staticmethod
    def _limpar_tree(tree):
        for item in tree.get_children():
            tree.delete(item)


if __name__ == "__main__":
    app = Aplicacao()
    app.mainloop()

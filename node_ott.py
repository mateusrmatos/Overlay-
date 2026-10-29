import socket
import threading
import sys
import time

class NodeOTT:
    def __init__(self, my_port, neighbors_ports):
        self.my_port = my_port
        self.neighbors_ports = neighbors_ports

    def start(self):
        # 1. Cria a thread paralela só para ficar à escuta
        thread_escuta = threading.Thread(target=self.escutar_conexoes)
        thread_escuta.daemon = True
        thread_escuta.start()

        # Dá tempo para abrires os terminais todos
        time.sleep(2) 

        # 2. Envia a mensagem simples de teste aos vizinhos
        self.enviar_hello()

        # Mantém o programa a correr
        while True:
            time.sleep(1)

    def escutar_conexoes(self):
        # Abre a porta TCP
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('127.0.0.1', self.my_port))
        server.listen(5)
        
        print(f"Nó à escuta na porta: {self.my_port}")

        while True:
            client_socket, address = server.accept()
            
            # Lê o texto que o vizinho enviou (até 1024 caracteres)
            mensagem = client_socket.recv(1024).decode('utf-8')
            
            if mensagem:
                print(f"-> Recebi: {mensagem}")
                
            client_socket.close()

    def enviar_hello(self):
        # Texto super simples
        texto = f"HELLO do no {self.my_port}"
        
        for vizinho in self.neighbors_ports:
            try:
                # Liga-se ao vizinho e envia o texto
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect(('127.0.0.1', vizinho))
                client_socket.send(texto.encode('utf-8'))
                client_socket.close()
            except ConnectionRefusedError:
                # Se o vizinho ainda não abriu o terminal, ignora o erro
                pass

if __name__ == "__main__":
    # Lê as portas diretamente do terminal do Ubuntu
    minha_porta = int(sys.argv[1])
    portas_vizinhos = [int(p) for p in sys.argv[2:]]

    node = NodeOTT(minha_porta, portas_vizinhos)
    node.start()
import time
import torch

def fBenchmarkMatMul(tamanho: int = 4096, repeticoes: int = 10):
    print(f"\n📐 Benchmark de multiplicação de matrizes: {tamanho}x{tamanho}, {repeticoes} repetições")

    a = torch.randn(tamanho, tamanho, device="cuda")
    b = torch.randn(tamanho, tamanho, device="cuda")

    # Aquece a GPU
    _ = a @ b
    torch.cuda.synchronize()

    tempos = []
    for _ in range(repeticoes):
        start = time.time()
        _ = a @ b
        torch.cuda.synchronize()
        tempos.append(time.time() - start)

    tempo_medio = sum(tempos) / repeticoes
    flops = 2 * (tamanho**3)  # operações em uma matmul: 2 * N³
    tflops = flops / tempo_medio / 1e12

    print(f"⏱️ Tempo médio: {tempo_medio:.6f} s")
    print(f"⚡ Estimativa: {tflops:.2f} TFLOPs")


def fBenchmarkMLP(batch_size: int = 1024, input_dim: int = 2048, hidden_dim: int = 4096, output_dim: int = 1000, repeticoes: int = 10):
    print(f"\n🧠 Benchmark de MLP: batch {batch_size}, input {input_dim}, hidden {hidden_dim}, output {output_dim}")

    model = torch.nn.Sequential(
        torch.nn.Linear(input_dim, hidden_dim),
        torch.nn.ReLU(),
        torch.nn.Linear(hidden_dim, output_dim)
    ).cuda()

    optimizer = torch.optim.SGD(model.parameters(), lr=0.01)
    x = torch.randn(batch_size, input_dim, device="cuda")
    y = torch.randn(batch_size, output_dim, device="cuda")

    # Aquece a GPU
    pred = model(x)
    loss = (pred - y).pow(2).mean()
    loss.backward()
    optimizer.step()
    torch.cuda.synchronize()

    tempos = []
    for _ in range(repeticoes):
        optimizer.zero_grad()
        start = time.time()
        pred = model(x)
        loss = (pred - y).pow(2).mean()
        loss.backward()
        optimizer.step()
        torch.cuda.synchronize()
        tempos.append(time.time() - start)

    tempo_medio = sum(tempos) / repeticoes
    print(f"⏱️ Tempo médio por batch: {tempo_medio:.6f} s")
    print(f"🔁 Iterações por segundo: {1 / tempo_medio:.2f} it/s")

if __name__ == "__main__":
    print(f"📦 PyTorch: {torch.__version__}")
    print(f"💻 GPU: {torch.cuda.get_device_name(0)}")

    fBenchmarkMatMul()
    fBenchmarkMLP()

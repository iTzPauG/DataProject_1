import subprocess
import sys

if __name__ == "__main__":
    # Lanzar ambos scripts en paralelo
    p1 = subprocess.Popen([sys.executable, "ingesta_valencia.py"])
    p2 = subprocess.Popen([sys.executable, "ingesta_madrid.py"])
    
    p1.wait()
    p2.wait()
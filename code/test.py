import numpy as np
from scipy import linalg
rng = np.random.default_rng()
m,n = 2300, 13840
a = rng.standard_normal((m, n))+ 1.j*rng.standard_normal((m,n))
U, s, Vh = linalg.svd(a)
print(U.shape, s.shape, Vh.shape)
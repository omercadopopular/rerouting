# -*- coding: utf-8 -*-
"""
Created on Sun Dec 29 11:00:00 2024

Plot cumulative welfare changes with styling similar to the EU‐prices figure.

"""

import os
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# === 1. RE‐USE OR LOAD YOUR computed data ===
# For this example, we'll just re‐compute a small toy version of grid/deltas.
# In practice, you would have run the previous integration code already.
# Here, we assume `grid` is a NumPy array of λ values, and `deltas` is the welfare change.

# --- Toy re‐computation (replace with your actual `grid, deltas`) ---
# (You can comment out this block if you already have `grid, deltas`.)
sigma   = 5.0
rho     = 1.0
alpha   = 0.5
c_f     = 1.0
mu      = sigma / (sigma - 1.0)

tau_fh_bar = 1.4
tauR_bar   = 1
eta        = 0.2
zeta       =     0.6

A_exp = eta * (sigma - 1.0) - 1.0
B_exp = zeta * (sigma - 1.0) - 1.0

def omega_star(lam):
    return ((lam * tau_fh_bar) / (mu * tauR_bar)) ** (1.0 / (eta - zeta))

def P_F_of_lambda(lam):
    w_raw = omega_star(lam)
    if w_raw <= 0:
        wstar = 0.0
    elif w_raw >= 1:
        wstar = 1.0
    else:
        wstar = w_raw

    direct_integral = (tau_fh_bar ** (1.0 - sigma)) * (wstar ** (A_exp + 1.0) / (A_exp + 1.0))
    if wstar >= 1.0:
        reroute_integral = 0.0
    else:
        reroute_integral = (tauR_bar ** (1.0 - sigma)) * ((1.0 - wstar ** (B_exp + 1.0)) / (B_exp + 1.0))

    term1 = (mu * c_f) ** (1.0 - sigma) * direct_integral
    term2 = (mu ** 2 * c_f) ** (1.0 - sigma) * reroute_integral

    val = term1 + term2
    return max(val, 0.0) ** (1.0 / (1.0 - sigma))

def S_F_of_lambda(lam):
    Pf = P_F_of_lambda(lam)
    num = (1.0 - alpha) * Pf ** (-(rho - 1.0))
    den = alpha + (1.0 - alpha) * Pf ** (-(rho - 1.0))
    return num / den

def s_fh_of_lambda(lam):
    w_raw = omega_star(lam)
    if w_raw >= 1.0:
        wstar = 1.0
        direct_int = (tau_fh_bar ** (1.0 - sigma)) * (1.0 ** (A_exp + 1.0) / (A_exp + 1.0))
        reroute_int = 0.0
    elif w_raw <= 0.0:
        wstar = 0.0
        direct_int = 0.0
        reroute_int = (tauR_bar ** (1.0 - sigma)) * ((1.0 - 0.0 ** (B_exp + 1.0)) / (B_exp + 1.0))
    else:
        wstar = w_raw
        direct_int = (tau_fh_bar ** (1.0 - sigma)) * (wstar ** (A_exp + 1.0) / (A_exp + 1.0))
        reroute_int = (tauR_bar ** (1.0 - sigma)) * ((1.0 - wstar ** (B_exp + 1.0)) / (B_exp + 1.0))

    A_term = (mu * c_f) ** (1.0 - sigma) * direct_int
    B_term = (mu ** 2 * c_f) ** (1.0 - sigma) * reroute_int
    if A_term + B_term == 0.0:
        return 0.0
    else:
        return A_term / (A_term + B_term)

def integrand_for_welfare(lam):
    return S_F_of_lambda(lam) * s_fh_of_lambda(lam) / lam

from scipy.integrate import quad

def delta_lnC(lambda0, lambda1):
    val, _ = quad(integrand_for_welfare, lambda0, lambda1, limit=200)
    return -val

# Generate grid and deltas
lambda0 = 1.0
grid = np.linspace(1.0, 3.0, 201)
deltas = [delta_lnC(lambda0, lam1) for lam1 in grid]

# === 2. PLOTTING ===

# Path and filename settings
path = r'C:\Users\cbezerradegoes\OneDrive\research\rerouting'
os.makedirs(path, exist_ok=True)         # create folder if it doesn't exist
outname = 'welfare_change_lambda.pdf'    # name of the PDF file

# Use Seaborn style
sns.set_style("whitegrid")
sns.set_context("talk", font_scale=1.0)

# Create figure
fig, ax = plt.subplots(1, 1, figsize=(10, 8))   # single panel

# Plot the welfare change line
ax.plot(grid, deltas, lw=3, color='navy')

# Horizontal zero line
ax.axhline(y=0, color='black', linewidth=1.2, linestyle='--')

# Titles and labels
#ax.set_title('Cumulative Change in Welfare\nas $\lambda$ moves from 1 to $\lambda$', 
#             fontsize=18, fontweight='bold')
ax.set_xlabel(r'$\lambda$', fontsize=14)
ax.set_ylabel(r'$\ln C(\lambda) - \ln C(1)$', fontsize=14)

# Tick parameters
ax.tick_params(axis='both', which='major', labelsize=14)

# Grid formatting (dashed lines at 60% opacity)
ax.grid(True, linestyle='--', alpha=0.6)

# Legend (if desired)
ax.legend(fontsize=14, frameon=False, loc='upper right')

# Tight layout and save
plt.tight_layout()
plt.savefig(os.path.join(path, outname), bbox_inches='tight')
plt.show()


# === 10. NEW PLOTS: S_F(λ) and s_fh(λ) as functions of λ ===

# Compute S_F and s_fh over the existing `grid` of λ values
S_F_vals  = [S_F_of_lambda(lam)  for lam in grid]
s_fh_vals = [s_fh_of_lambda(lam) for lam in grid]

# Create a figure with two subplots side by side
fig, axes = plt.subplots(1, 2, figsize=(16, 6), sharey=False)

# Common styling for both panels
for ax in axes:
    ax.grid(visible=True, linestyle='--', alpha=0.6)
    ax.set_xlabel(r'$\lambda$', fontsize=14)
    ax.tick_params(axis='both', which='major', labelsize=14)
    ax.axhline(y=0, color='black', linewidth=1, linestyle='-')
    ax.axhline(y=1, color='black', linewidth=1, linestyle='-')

# Panel A: S_F(λ)
axes[0].plot(grid, S_F_vals, color='darkgreen', linewidth=2)
axes[0].set_title(r'Foreign‐Goods Share $S_F(\lambda)$', fontsize=18, fontweight='bold')
axes[0].set_ylabel(r'$S_F(\lambda)$', fontsize=14)

# Panel B: s_fh(λ)
axes[1].plot(grid, s_fh_vals, color='royalblue', linewidth=2)
axes[1].set_title(r'Direct‐Shipping Share $s_{fh}(\lambda)$', fontsize=18, fontweight='bold')
axes[1].set_ylabel(r'$s_{fh}(\lambda)$', fontsize=14)

# Tight layout and save
plt.tight_layout()
plt.savefig(os.path.join(path, 'figs/SF_and_sfh_vs_lambda.pdf'), bbox_inches='tight')
plt.show()







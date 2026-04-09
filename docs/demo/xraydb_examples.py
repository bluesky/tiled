"""
Load XrayDB X-ray reference data into Tiled.

Creates a container 'examples/xraydb' with one sub-container per element (Z=1..98).
Each element container holds:
  - An array 'mu_total' : mass attenuation coefficient vs energy (from Chantler tables)
  - A table 'emission_lines' : all X-ray emission lines for the element
  - A table 'edges' : X-ray absorption edges for the element

Metadata on each element container includes atomic properties and summary info
useful for demonstrating Tiled's search capabilities.

Requirements:
    pip install xraydb numpy pandas tiled
"""

import numpy as np
import pandas as pd
import xraydb

from tiled.client import from_uri

TILED_URI = "https://tiled-demo.nsls2.bnl.gov"

# Periodic-table groupings for searchable metadata
ELEMENT_CATEGORIES = {
    "H": "nonmetal",
    "He": "noble_gas",
    "Li": "alkali_metal",
    "Be": "alkaline_earth",
    "B": "metalloid",
    "C": "nonmetal",
    "N": "nonmetal",
    "O": "nonmetal",
    "F": "halogen",
    "Ne": "noble_gas",
    "Na": "alkali_metal",
    "Mg": "alkaline_earth",
    "Al": "post_transition_metal",
    "Si": "metalloid",
    "P": "nonmetal",
    "S": "nonmetal",
    "Cl": "halogen",
    "Ar": "noble_gas",
    "K": "alkali_metal",
    "Ca": "alkaline_earth",
    "Sc": "transition_metal",
    "Ti": "transition_metal",
    "V": "transition_metal",
    "Cr": "transition_metal",
    "Mn": "transition_metal",
    "Fe": "transition_metal",
    "Co": "transition_metal",
    "Ni": "transition_metal",
    "Cu": "transition_metal",
    "Zn": "transition_metal",
    "Ga": "post_transition_metal",
    "Ge": "metalloid",
    "As": "metalloid",
    "Se": "nonmetal",
    "Br": "halogen",
    "Kr": "noble_gas",
    "Rb": "alkali_metal",
    "Sr": "alkaline_earth",
    "Y": "transition_metal",
    "Zr": "transition_metal",
    "Nb": "transition_metal",
    "Mo": "transition_metal",
    "Tc": "transition_metal",
    "Ru": "transition_metal",
    "Rh": "transition_metal",
    "Pd": "transition_metal",
    "Ag": "transition_metal",
    "Cd": "transition_metal",
    "In": "post_transition_metal",
    "Sn": "post_transition_metal",
    "Sb": "metalloid",
    "Te": "metalloid",
    "I": "halogen",
    "Xe": "noble_gas",
    "Cs": "alkali_metal",
    "Ba": "alkaline_earth",
    "La": "lanthanide",
    "Ce": "lanthanide",
    "Pr": "lanthanide",
    "Nd": "lanthanide",
    "Pm": "lanthanide",
    "Sm": "lanthanide",
    "Eu": "lanthanide",
    "Gd": "lanthanide",
    "Tb": "lanthanide",
    "Dy": "lanthanide",
    "Ho": "lanthanide",
    "Er": "lanthanide",
    "Tm": "lanthanide",
    "Yb": "lanthanide",
    "Lu": "lanthanide",
    "Hf": "transition_metal",
    "Ta": "transition_metal",
    "W": "transition_metal",
    "Re": "transition_metal",
    "Os": "transition_metal",
    "Ir": "transition_metal",
    "Pt": "transition_metal",
    "Au": "transition_metal",
    "Hg": "transition_metal",
    "Tl": "post_transition_metal",
    "Pb": "post_transition_metal",
    "Bi": "post_transition_metal",
    "Po": "post_transition_metal",
    "At": "halogen",
    "Rn": "noble_gas",
    "Fr": "alkali_metal",
    "Ra": "alkaline_earth",
    "Ac": "actinide",
    "Th": "actinide",
    "Pa": "actinide",
    "U": "actinide",
    "Np": "actinide",
    "Pu": "actinide",
    "Am": "actinide",
    "Cm": "actinide",
    "Bk": "actinide",
    "Cf": "actinide",
}

PERIODS = {
    1: (1, 2),
    2: (3, 10),
    3: (11, 18),
    4: (19, 36),
    5: (37, 54),
    6: (55, 86),
    7: (87, 98),
}


def get_period(z):
    for period, (lo, hi) in PERIODS.items():
        if lo <= z <= hi:
            return period
    return None


def build_element_metadata(z):
    """Build a rich metadata dict for one element."""
    sym = xraydb.atomic_symbol(z)
    meta = {
        "element": {
            "symbol": sym,
            "name": xraydb.atomic_name(z),
            "atomic_number": z,
            "molar_mass": round(xraydb.atomic_mass(z), 4),
            "period": get_period(z),
            "category": ELEMENT_CATEGORIES.get(sym, "unknown"),
        },
    }

    # Density (may be 0 for some elements)
    density = xraydb.atomic_density(z)
    if density:
        meta["element"]["density_g_cm3"] = round(density, 4)

    # Summarize edges so they are searchable in metadata
    edges = xraydb.xray_edges(z)
    if edges:
        edge_summary = {}
        for edge_name, edge_data in edges.items():
            edge_summary[edge_name] = {
                "energy_eV": round(edge_data.energy, 2),
                "fluorescence_yield": round(edge_data.fyield, 6),
                "jump_ratio": round(edge_data.jump_ratio, 4),
            }
        meta["edges"] = edge_summary

    # Summarize strongest emission lines
    lines = xraydb.xray_lines(z)
    if lines:
        strongest = sorted(lines.items(), key=lambda x: -x[1].intensity)[:5]
        meta["strongest_lines"] = {
            name: {
                "energy_eV": round(line.energy, 2),
                "intensity": round(line.intensity, 6),
            }
            for name, line in strongest
        }

    return meta


def build_mu_array(z):
    """
    Build mass attenuation coefficient array over the Chantler energy grid.
    Returns (energies, mu_total) as a 2D array of shape (2, N).
    Returns None if data is not available for this element.
    """
    sym = xraydb.atomic_symbol(z)
    try:
        energies = xraydb.chantler_energies(sym, emin=100, emax=300_000)
    except Exception:
        return None
    if len(energies) == 0:
        return None
    try:
        mu = xraydb.mu_elam(z, energies, kind="total")
    except Exception:
        return None
    # Stack into (2, N) — row 0 = energy_eV, row 1 = mu_total_cm2_per_g
    return np.vstack([energies, mu])


def build_emission_lines_table(z):
    """Build a DataFrame of all emission lines for an element."""
    lines = xraydb.xray_lines(z)
    if not lines:
        return None
    rows = []
    for name, line in lines.items():
        rows.append(
            {
                "line": name,
                "energy_eV": round(line.energy, 2),
                "intensity": round(line.intensity, 6),
                "initial_level": line.initial_level,
                "final_level": line.final_level,
            }
        )
    return pd.DataFrame(rows)


def build_edges_table(z):
    """Build a DataFrame of absorption edges for an element."""
    edges = xraydb.xray_edges(z)
    if not edges:
        return None
    rows = []
    for edge_name, edge_data in edges.items():
        rows.append(
            {
                "edge": edge_name,
                "energy_eV": round(edge_data.energy, 2),
                "fluorescence_yield": round(edge_data.fyield, 6),
                "jump_ratio": round(edge_data.jump_ratio, 4),
            }
        )
    return pd.DataFrame(rows)


def main():
    c = from_uri(TILED_URI)

    # Clean up previous run
    if "examples" in c and "xraydb" in c["examples"]:
        c["examples/xraydb"].delete(recursive=True, external_only=False)
    if "examples" not in c:
        c.create_container("examples")
    xraydb_container = c["examples"].create_container(
        "xraydb",
        metadata={
            "description": (
                "X-ray reference data for the elements, sourced from XrayDB "
                "(https://github.com/xraypy/XrayDB). Includes mass attenuation "
                "coefficients, emission lines, and absorption edges."
            ),
            "source": "xraydb",
            "version": "9.2",
            "energy_units": "eV",
            "mu_units": "cm^2/g",
        },
    )

    for z in range(1, 99):  # H (1) through Cf (98)
        sym = xraydb.atomic_symbol(z)
        name = xraydb.atomic_name(z)
        print(f"  [{z:>2}/{98}] {sym} ({name})")

        meta = build_element_metadata(z)

        # Create a container for this element
        elem = xraydb_container.create_container(sym, metadata=meta)

        # Write the mu(E) array
        mu_arr = build_mu_array(z)
        if mu_arr is not None:
            elem.write_array(
                mu_arr,
                key="mu_total",
                metadata={
                    "description": (
                        "Mass attenuation coefficient (total) vs photon energy. "
                        "Row 0: energy (eV), Row 1: mu/rho (cm^2/g)."
                    ),
                    "source": "Chantler tables via xraydb.mu_elam",
                },
            )

        # Write emission lines table
        lines_df = build_emission_lines_table(z)
        if lines_df is not None:
            elem.write_table(
                lines_df,
                key="emission_lines",
                metadata={
                    "description": (
                        "Characteristic X-ray emission lines. "
                        "Energies in eV; intensity is relative within a given initial level."
                    ),
                },
            )

        # Write edges table
        edges_df = build_edges_table(z)
        if edges_df is not None:
            elem.write_table(
                edges_df,
                key="edges",
                metadata={
                    "description": (
                        "X-ray absorption edges. Energies in eV. "
                        "Fluorescence yield: probability of refilling by X-ray emission. "
                        "Jump ratio: ratio of mu above/below the edge."
                    ),
                },
            )

    print(f"\nDone. Wrote {98} elements to {TILED_URI} under examples/xraydb.")
    print("Total nodes: 98 element containers + up to 3 datasets each " "≈ ~390 nodes.")


if __name__ == "__main__":
    main()

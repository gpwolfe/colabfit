polarizability_pd = {
    "property-id": "tag:staff@noreply.colabfit.org,2022-05-30:property/polarizability",
    "property-name": "polarizability",
    "property-title": "Polarizability",
    "property-description": "Polarizability of a molecule",
    "polarizability": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": False,
        "description": "Polarizability of the molecule.",
    },
    "di-quad": {
        "type": "string",
        "has-unit": False,
        "extent": [],
        "required": False,
        "description": "Whether polarizability is [dipole] or [quadrupole]",
    },
    "iso-aniso": {
        "type": "string",
        "has-unit": False,
        "extent": [],
        "required": False,
        "description": "Whether polarizability is [isotropic] or [anisotropic]",
    },
    "per-atom": {
        "type": "bool",
        "has-unit": False,
        "extent": [],
        "required": False,
        "description": ".",
    },
}


dipole_moment_pd = {
    "property-id": "tag:staff@noreply.colabfit.org,2022-05-30:property/dipole-moment",
    "property-name": "dipole-moment",
    "property-title": "Dipole moment of a molecule",
    "property-description": "The vector sum of dipole moments for all bonds in a molecule.",
    "dipole-moment": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": False,
        "description": "Dipole moment of the molecule.",
    },
    "per-atom": {
        "type": "bool",
        "has-unit": False,
        "extent": [],
        "required": False,
        "description": ".",
    },
}

dipole_polarizability_pd = {
    "property-id": "tag:staff@noreply.colabfit.org,2022-05-30:property/dipole-polarizability",
    "property-name": "dipole-polarizability",
    "property-title": "Dipole polarizability",
    "property-description": "Dipole polarizability of a molecule",
    "dipole-polarizability": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": False,
        "description": "Dipole polarizability of the molecule.",
    },
    "per-atom": {
        "type": "bool",
        "has-unit": False,
        "extent": [],
        "required": False,
        "description": ".",
    },
}

lumo_energy_pd = {
    "property-id": "tag:staff@noreply.colabfit.org,2022-05-30:property/lumo-energy",
    "property-name": "lumo-energy",
    "property-title": "Lowest unoccupied molecular orbital (LUMO)",
    "property-description": "Energy of the lowest unoccupied molecular orbital (LUMO)",
    "lumo-energy": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": False,
        "description": "Energy of the lowest unoccupied molecular orbital (LUMO).",
    },
    "per-atom": {
        "type": "bool",
        "has-unit": False,
        "extent": [],
        "required": False,
        "description": ".",
    },
}

homo_energy_pd = {
    "property-id": "tag:staff@noreply.colabfit.org,2022-05-30:property/homo-energy",
    "property-name": "homo-energy",
    "property-title": "Highest occupied molecular orbital (HOMO)",
    "property-description": "Energy of the highest occupied molecular orbital (HOMO)",
    "homo-energy": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": False,
        "description": "Energy of the highest occupied molecular orbital (HOMO).",
    },
    "per-atom": {
        "type": "bool",
        "has-unit": False,
        "extent": [],
        "required": False,
        "description": ".",
    },
}

isotropic_polarizability_pd = {
    "property-id": "tag:staff@noreply.colabfit.org,2022-05-30:property/isotropic-polarizability",
    "property-name": "isotropic-polarizability",
    "property-title": "Isotropic polarizability",
    "property-description": "Polarizability of a molecule",
    "isotropic-polarizability": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": False,
        "description": "Polarizability of a molecule.",
    },
    "per-atom": {
        "type": "bool",
        "has-unit": False,
        "extent": [],
        "required": False,
        "description": ".",
    },
}

homo_lumo_gap_pd = {
    "property-id": "tag:staff@noreply.colabfit.org,2022-05-30:property/homo-lumo-gap",
    "property-name": "homo-lumo-gap",
    "property-title": "HOMO-LUMO gap",
    "property-description": "Difference in energy between the highest occupied molecular orbit (HOMO) and the lowest unoccupied molecular orbital (LUMO)",
    "homo-lumo-gap": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": False,
        "description": "Difference in energy between the highest occupied molecular orbit (HOMO) and the lowest unoccupied molecular orbital (LUMO)",
    },
    "per-atom": {
        "type": "bool",
        "has-unit": False,
        "extent": [],
        "required": False,
        "description": ".",
    },
}

electronic_spatial_extent_pd = {
    "property-id": "tag:staff@noreply.colabfit.org,2022-05-30:property/electronic-spatial-extent",
    "property-name": "electronic-spatial-extent",
    "property-title": "Electronic spatial extent",
    "property-description": "A measure of the size of a molecule, using electron density and distance from molecular center of mass",
    "electronic-spatial-extent": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": False,
        "description": "A measure of the size of a molecule, using electron density and distance from molecular center of mass",
    },
    "per-atom": {
        "type": "bool",
        "has-unit": False,
        "extent": [],
        "required": False,
        "description": ".",
    },
}

zpve_pd = {
    "property-id": "tag:staff@noreply.colabfit.org,2022-05-30:property/zpve",
    "property-name": "zpve",
    "property-title": "Zero-point vibrational energy",
    "property-description": "Calculated vibrational energy in a molecule at 0 degrees Kelvin",
    "zpve": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": False,
        "description": "Calculated vibrational energy in a molecule at 0 degrees Kelvin",
    },
    "per-atom": {
        "type": "bool",
        "has-unit": False,
        "extent": [],
        "required": False,
        "description": ".",
    },
}

internal_energy_pd = {
    "property-id": "tag:staff@noreply.colabfit.org,2022-05-30:property/internal-energy",
    "property-name": "internal-energy",
    "property-title": "Internal energy",
    "property-description": "Internal energy of a molecule at defined temperature",
    "internal-energy": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": False,
        "description": "Internal energy of a molecule",
    },
    "temperature": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": True,
        "description": "Temperature for which internal energy was calculated.",
    },
    "per-atom": {
        "type": "bool",
        "has-unit": False,
        "extent": [],
        "required": False,
        "description": ".",
    },
}

enthalpy_pd = {
    "property-id": "tag:staff@noreply.colabfit.org,2022-05-30:property/enthalpy",
    "property-name": "enthalpy",
    "property-title": "Enthalpy",
    "property-description": "The sum of internal energy of the molecular system",
    "enthalpy": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": False,
        "description": "The enthalpy of the molecular system",
    },
    "temperature": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": True,
        "description": "Temperature for which enthalpy was calculated.",
    },
    "per-atom": {
        "type": "bool",
        "has-unit": False,
        "extent": [],
        "required": False,
        "description": ".",
    },
}

free_energy_pd = {
    "property-id": "tag:staff@noreply.colabfit.org,2022-05-30:property/free-energy",
    "property-name": "free-energy",
    "property-title": "Free energy from a static calculation",
    "property-description": "Free energy from a calculation of a static configuration. Energies must be specified to be per-atom or supercell. If a reference energy has been used, this must be specified as well.",
    "energy": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": False,
        "description": "The free energy of the system.",
    },
    "per-atom": {
        "type": "bool",
        "has-unit": False,
        "extent": [],
        "required": True,
        "description": 'If True, "energy" is the total energy of the system, and has NOT been divided by the number of atoms in the configuration.',
    },
    "temperature": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": True,
        "description": "Temperature for which free energy was calculated.",
    },
    "reference-energy": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": False,
        "description": 'If provided, then "energy" is the energy (either of the whole system, or per-atom) LESS the energy of a reference configuration (E = E_0 - E_reference). Note that "reference-energy" is just provided for documentation, and that "energy" should already have this value subtracted off. The reference energy must have the same units as "energy".',
    },
}

heat_capacity_pd = {
    "property-id": "tag:staff@noreply.colabfit.org,2022-05-30:property/heat-capacity",
    "property-name": "heat-capacity",
    "property-title": "Heat capacity",
    "property-description": "Heat capacity of a molecule",
    "heat-capacity": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": False,
        "description": "The heat capacity of a molecule",
    },
    "temperature": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": True,
        "description": "Temperature for which heat capacity was calculated.",
    },
    "per-atom": {
        "type": "bool",
        "has-unit": False,
        "extent": [],
        "required": False,
        "description": ".",
    },
}

total_energy_pd = {
    "property-id": "tag:staff@noreply.colabfit.org,2022-05-30:property/total-energy",
    "property-name": "total-energy",
    "property-title": "Total energy",
    "property-description": "Total energy of a molecule",
    "energy": {
        "type": "float",
        "has-unit": True,
        "extent": [],
        "required": False,
        "description": "The total energy of a molecule",
    },
    "per-atom": {
        "type": "bool",
        "has-unit": False,
        "extent": [],
        "required": False,
        "description": ".",
    },
}

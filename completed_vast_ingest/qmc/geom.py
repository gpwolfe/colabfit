"""From dataset authors"""

import numpy as np
import ase
import ase.build


def get_monolayer(symbol, a, n, vacuum=None):
    """
    https://wiki.fysik.dtu.dk/ase/_modules/ase/build/surface.html
    Builds a monolayer honeycomb structure

    Args:
        symbol: 'C' or 'BN'
        a: lattice constant (ang),
            - BN lattice constant = 2.504 [https://doi.org/10.1016/j.materresbull.2015.06.032]
            - graphene lattice constant = 2.46
        n: supercell

    Returns:
        ase Atoms object
    """
    atoms = ase.build.graphene(symbol, a=a, size=(n, n, 1), vacuum=vacuum)
    return atoms


def get_bilayer(symbol1, symbol2, d, s, h, a, n):
    """
    Builds a bilayer system

    Args:
        symbol1: symbol of the first layer, e.g. 'C2'
        symbol2: symbol of the second layer, e.g. 'BN'
        d (ang): separation between layers
        s (ang): in-plane translation vectors of in ecah layer from AA stacking
        h (ang): height of the simulation cell
        a (ang): in-plane lattice constant
        n: defines a (n, n, 1) supercell

    Returns:
        ase Atoms object
    """
    nlayers = len(s)
    for i in range(nlayers):
        if i == 0:
            atoms = get_monolayer(symbol1, a=a, n=n)
            mol_id_list = np.array([i] * atoms.positions.shape[0])
            atoms.set_array("mol-id", mol_id_list)

        else:
            layer = get_monolayer(symbol2, a=a, n=n)
            pos = layer.get_positions()
            slide = s[i]
            pos[:, 0] += slide[0]
            pos[:, 1] += slide[1]
            pos[:, 2] += d * i
            layer.set_positions(pos)
            mol_id_list = np.array([i] * layer.positions.shape[0])
            layer.set_array("mol-id", mol_id_list)
            atoms += layer
    cell = atoms.get_cell()
    cell[2, 2] = h

    atoms.set_cell(cell)
    cm_curr = (nlayers - 1) * d / 2
    cm_target = h / 2
    z_move = cm_target - cm_curr
    pos = atoms.get_positions()
    pos[:, 2] += z_move
    atoms.set_positions(pos)
    return atoms


def create_geom(registry, d, a, h=40, n=1, basis="BN"):
    """
    Wrapper of `get_bilayer`

    Args:
        registry (unitless float): defines stacking types
            0: 'AB'
            0.16667: 'SP'
            0.33333: 'BA'
            0.66667: 'AA'
            1: 'AB'
        d (ang): separation between layers
        a (ang): in-plane lattice constant
        h (ang): height of the simulation cell
        n: defines a (n, n, 1) supercell

    Returns:
        ase Atoms object
    """
    registry_ang = (-2 / 3 + registry) * 3**0.5 * a
    s = [[0, 0], [0, registry_ang]]
    atoms = get_bilayer("C2", basis, d, s, h, a, n)
    atoms.set_pbc([1, 1, 1])
    return atoms


if __name__ == "__main__":
    s = 0.0
    d = 3.33
    a = 2.504
    h = 40
    atoms = create_geom(0.0, d, a, h=40, n=3)
    ase.io.write("test.xsf", atoms)

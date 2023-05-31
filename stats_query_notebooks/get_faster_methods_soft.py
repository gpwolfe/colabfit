from collections import defaultdict
from pymongo import MongoClient
from tqdm import tqdm

client = MongoClient("mongodb://localhost:5000/")
db = client["colabfit-2023-5-16"]
pi_types = [
    "atomic-forces",
    "potential-energy",
    "formation-energy",
    "atomization-energy",
    "band-gap",
    "cauchy-stress",
    "free-energy",
]
ds_ids = [
    "DS_sbwtdhegp9yg_0",
    "DS_k7pbw3d3i9a0_0",
    "DS_p6m0q7c6dd64_0",
    "DS_1dmkde5c5t51_0",
    "DS_701u8ywjlobm_0",
    "DS_ntkcb1ar94p1_0",
    "DS_owodx9hsaoy7_0",
    "DS_qhoalrlhv4ks_0",
    "DS_jznr39xgv01r_0",
    "DS_a02gyq0s18f6_0",
    "DS_tm6zdsxf8quy_0",
    "DS_ogli92hqsbxc_0",
    "DS_idvogv8o1igi_0",
    "DS_3vw7rv1mpzaj_0",
    "DS_3csidnzpm4sj_0",
    "DS_egwhckjzeld1_0",
    "DS_tj745ltugwxi_0",
    "DS_nsjxpjq9h6vb_0",
    "DS_2gqymct47qzy_0",
    "DS_cz1uojrptan7_0",
    "DS_pdgt4c0bwvj7_0",
    "DS_dlthpit74i1o_0",
    "DS_skirjrcwxg6j_0",
    "DS_mbmetlz6amcn_0",
    "DS_2no6367xt2vx_0",
    "DS_yr5p3nnpsga1_0",
    "DS_0mernrf40gvu_0",
    "DS_qeu0cnrepjm6_0",
    "DS_93dipny9hm81_0",
    "DS_9hnebjrtcgqc_0",
    "DS_g1qtwfvu3l7d_0",
    "DS_6zb4ilg860c4_0",
    "DS_3sx3kstypy96_0",
    "DS_j7uq9dw56pla_0",
    "DS_5y5ztxqtjd2t_0",
    "DS_ot9xol68jdhf_0",
    "DS_g61ty7dair9g_0",
    "DS_hsbqkkejpsa4_0",
    "DS_n3krc6uvqtap_0",
    "DS_5r77yzze9388_0",
    "DS_h4j30h2am8t0_0",
    "DS_8xosckbpt9xc_0",
    "DS_2ftfae4zwxoh_0",
    "DS_bx27t11tquwh_0",
    "DS_j9tkf6d96l9a_0",
    "DS_anlr0kza0thi_0",
    "DS_09uoc3bcuu0k_0",
    "DS_4wsbi56a4ca5_0",
    "DS_5irytpbn7ai5_0",
    "DS_tna2mx1bvhme_0",
    "DS_mfzde6so507t_0",
    "DS_c6wa3lfok4cy_0",
    "DS_ssa4v7h6jo35_0",
    "DS_nekp2oe3glgz_0",
    "DS_ygr46u2jr68w_0",
    "DS_k69cgxellqb5_0",
    "DS_pjo5z4bgtqnj_0",
    "DS_vum9u4p5219n_0",
    "DS_c86xyxz9e408_0",
    "DS_f57o6j3d4mct_0",
    "DS_86e0tn1jneh0_0",
    "DS_v0wtxa2oi4k6_0",
    "DS_yngv2ioojq5u_0",
    "DS_zma39ct7rimr_0",
    "DS_7jfcc8b0v3dr_0",
    "DS_8yy1q267djbb_0",
    "DS_ntrgw1fof9kg_0",
    "DS_xbt1nhjzfeuz_0",
    "DS_5ui3gln5rqsc_0",
    "DS_q0yv78azzncx_0",
    "DS_5ef2lepjdd4o_0",
    "DS_d5chcty87bi1_0",
    "DS_4k87utx298pn_0",
    "DS_242b0a2841sp_0",
    "DS_wukbsh7bvxvv_0",
    "DS_lbmoqe6piamk_0",
    "DS_jl9rksadf445_0",
    "DS_5zvnxnucx7wi_0",
    "DS_ucclki7ekavl_0",
    "DS_o9ojldif35dt_0",
    "DS_5o3t2tiwazgv_0",
    "DS_dyqp0sylx9ns_0",
    "DS_rjgvmczpgi9e_0",
    "DS_4rstlrhavluv_0",
    "DS_mdgjzhc44edz_0",
    "DS_wr1zmzj7kbq3_0",
    "DS_i2xbnvrzq2yp_0",
    "DS_wsngj1rf6bo7_0",
    "DS_l2yam3z2d124_0",
    "DS_zqqqnnq4oewb_0",
    "DS_acu98g19jdud_0",
    "DS_qn3ko3gm3wor_0",
    "DS_rh95xq3jofug_0",
    "DS_bdxywgkyw3la_0",
    "DS_u8zf1fceg0ey_0",
    "DS_0udxwc0aimbe_0",
    "DS_tsod7jyslzqu_0",
    "DS_qhxd0whlfube_0",
    "DS_9fla74bvjph7_0",
    "DS_nxhdh4vk6n1d_0",
    "DS_tzuvscuoanfm_0",
    "DS_3m21p7i6i2gd_0",
    "DS_tuesxewmus7c_0",
    "DS_4y7n0oevj84o_0",
    "DS_7urj8ubwavdv_0",
    "DS_2wiwpg3oyupj_0",
]
methods = defaultdict(int)
software = defaultdict(int)
for pi in tqdm(db.property_instances.find({},{"relationships.data_objects":1, "colabfit-id":1, "relationships.metadata":1, "type":1})):
    dos = pi['relationships']['data_objects']
    for do_id in dos:
        do = db.data_objects.find_one(
            {"colabfit-id": do_id}, {"relationships.datasets": 1}
        )
        if do["relationships"]["datasets"][0] in ds_ids:
            mds = pi['relationships']['metadata']
            md = db.metadata.find_one(
                {
                    "colabfit-id": mds[0],
                }
            )
            meth = md.get("method")
            if meth:
                methods[f"{pi['type']}__{meth['source-value']}"] += 1
            soft = md.get("software")
            if soft:
                software[f"{pi['type']}__{soft['source-value']}"] += 1
with open("methods_no_oc2.txt", "w") as f:
    f.write(str(methods))
with open("software_no_oc2.txt", "w") as f:
    f.write(str(software))

// Palette colors adapted to OneTUI roles. Sources and notices: ../README.md and ../../../THIRD_PARTY_NOTICES.md.
use crate::Palette;

const fn rgb(hex: u32) -> [u8; 3] {
    [(hex >> 16) as u8, (hex >> 8) as u8, hex as u8]
}

pub const CATPPUCCIN: Palette = Palette {
    background: rgb(0x1e_1e_2e),
    surface: rgb(0x18_18_25),
    text: rgb(0xcd_d6_f4),
    muted: rgb(0xa6_ad_c8),
    border: rgb(0xb4_be_fe),
    title: rgb(0x89_dc_eb),
    key_hint: rgb(0x89_dc_eb),
    identifier: rgb(0x89_b4_fa),
    table_heading: rgb(0xf9_e2_af),
    selection_fg: rgb(0x1e_1e_2e),
    selection_bg: rgb(0xb4_be_fe),
    success: rgb(0xa6_e3_a1),
    warning: rgb(0xf9_e2_af),
    error: rgb(0xf3_8b_a8),
};

pub const GRUVBOX: Palette = Palette {
    background: rgb(0x28_28_28),
    surface: rgb(0x3c_38_36),
    text: rgb(0xeb_db_b2),
    muted: rgb(0xbd_ae_93),
    border: rgb(0xd3_86_9b),
    title: rgb(0x8e_c0_7c),
    key_hint: rgb(0x8e_c0_7c),
    identifier: rgb(0x83_a5_98),
    table_heading: rgb(0xfa_bd_2f),
    selection_fg: rgb(0x28_28_28),
    selection_bg: rgb(0xd3_86_9b),
    success: rgb(0xb8_bb_26),
    warning: rgb(0xfa_bd_2f),
    error: rgb(0xfb_49_34),
};

pub const SOLARIZED: Palette = Palette {
    background: rgb(0x00_2b_36),
    surface: rgb(0x07_36_42),
    text: rgb(0x93_a1_a1),
    muted: rgb(0x83_94_96),
    border: rgb(0x93_a1_a1),
    title: rgb(0x2a_a1_98),
    key_hint: rgb(0x2a_a1_98),
    identifier: rgb(0x26_8b_d2),
    table_heading: rgb(0xb5_89_00),
    selection_fg: rgb(0x00_2b_36),
    selection_bg: rgb(0x93_a1_a1),
    success: rgb(0x85_99_00),
    warning: rgb(0xb5_89_00),
    error: rgb(0xdc_32_2f),
};

pub const NORD: Palette = Palette {
    background: rgb(0x2e_34_40),
    surface: rgb(0x3b_42_52),
    text: rgb(0xec_ef_f4),
    muted: rgb(0x81_a1_c1),
    border: rgb(0x88_c0_d0),
    title: rgb(0x8f_bc_bb),
    key_hint: rgb(0x88_c0_d0),
    identifier: rgb(0x81_a1_c1),
    table_heading: rgb(0xeb_cb_8b),
    selection_fg: rgb(0x2e_34_40),
    selection_bg: rgb(0x88_c0_d0),
    success: rgb(0xa3_be_8c),
    warning: rgb(0xeb_cb_8b),
    error: rgb(0xbf_61_6a),
};

pub const DRACULA: Palette = Palette {
    background: rgb(0x28_2a_36),
    surface: rgb(0x44_47_5a),
    text: rgb(0xf8_f8_f2),
    muted: rgb(0xbd_93_f9),
    border: rgb(0xbd_93_f9),
    title: rgb(0x8b_e9_fd),
    key_hint: rgb(0x8b_e9_fd),
    identifier: rgb(0xbd_93_f9),
    table_heading: rgb(0xf1_fa_8c),
    selection_fg: rgb(0x28_2a_36),
    selection_bg: rgb(0xbd_93_f9),
    success: rgb(0x50_fa_7b),
    warning: rgb(0xf1_fa_8c),
    error: rgb(0xff_55_55),
};

pub const TOKYO_NIGHT: Palette = Palette {
    background: rgb(0x1a_1b_26),
    surface: rgb(0x16_16_1e),
    text: rgb(0xc0_ca_f5),
    muted: rgb(0xa9_b1_d6),
    border: rgb(0xbb_9a_f7),
    title: rgb(0x7d_cf_ff),
    key_hint: rgb(0x7d_cf_ff),
    identifier: rgb(0x7a_a2_f7),
    table_heading: rgb(0xe0_af_68),
    selection_fg: rgb(0x1a_1b_26),
    selection_bg: rgb(0xbb_9a_f7),
    success: rgb(0x9e_ce_6a),
    warning: rgb(0xe0_af_68),
    error: rgb(0xf7_76_8e),
};

pub const ONE_DARK: Palette = Palette {
    background: rgb(0x28_2c_34),
    surface: rgb(0x21_25_2b),
    text: rgb(0xab_b2_bf),
    muted: rgb(0x82_89_97),
    border: rgb(0xc6_78_dd),
    title: rgb(0x56_b6_c2),
    key_hint: rgb(0x56_b6_c2),
    identifier: rgb(0x61_af_ef),
    table_heading: rgb(0xe5_c0_7b),
    selection_fg: rgb(0x28_2c_34),
    selection_bg: rgb(0xc6_78_dd),
    success: rgb(0x98_c3_79),
    warning: rgb(0xe5_c0_7b),
    error: rgb(0xe0_6c_75),
};

pub const ROSE_PINE: Palette = Palette {
    background: rgb(0x19_17_24),
    surface: rgb(0x1f_1d_2e),
    text: rgb(0xe0_de_f4),
    muted: rgb(0x90_8c_aa),
    border: rgb(0xc4_a7_e7),
    title: rgb(0xeb_bc_ba),
    key_hint: rgb(0x9c_cf_d8),
    identifier: rgb(0x9c_cf_d8),
    table_heading: rgb(0xf6_c1_77),
    selection_fg: rgb(0x19_17_24),
    selection_bg: rgb(0xc4_a7_e7),
    success: rgb(0x95_b1_ac),
    warning: rgb(0xf6_c1_77),
    error: rgb(0xeb_6f_92),
};

pub const MONOKAI: Palette = Palette {
    background: rgb(0x27_28_22),
    surface: rgb(0x1e_1f_1c),
    text: rgb(0xf8_f8_f2),
    muted: rgb(0x90_90_8a),
    border: rgb(0xae_81_ff),
    title: rgb(0x66_d9_ef),
    key_hint: rgb(0x66_d9_ef),
    identifier: rgb(0x66_d9_ef),
    table_heading: rgb(0xe6_db_74),
    selection_fg: rgb(0x27_28_22),
    selection_bg: rgb(0xae_81_ff),
    success: rgb(0xa6_e2_2e),
    warning: rgb(0xe6_db_74),
    error: rgb(0xf9_26_72),
};

pub const FLEXOKI: Palette = Palette {
    background: rgb(0x10_0f_0f),
    surface: rgb(0x1c_1b_1a),
    text: rgb(0xce_cd_c3),
    muted: rgb(0x87_85_80),
    border: rgb(0x8b_7e_c8),
    title: rgb(0x3a_a9_9f),
    key_hint: rgb(0x3a_a9_9f),
    identifier: rgb(0x43_85_be),
    table_heading: rgb(0xd0_a2_15),
    selection_fg: rgb(0x10_0f_0f),
    selection_bg: rgb(0x8b_7e_c8),
    success: rgb(0x87_9a_39),
    warning: rgb(0xd0_a2_15),
    error: rgb(0xd1_4d_41),
};

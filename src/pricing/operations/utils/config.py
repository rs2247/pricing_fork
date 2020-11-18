
EXCEPTIONS = {
  'full_fixed': '0', # APLICADO APENAS A GRADUACAO!
  'full_fixed_presencial': '1360', # APLICADO APENAS A GRADUACAO!, PUCPR fixa em full
  '1_5_full_fixed_presencial': '0',
  'full_fixed_pos_vitrine': '0',
  'full_fixed_pos': '0',
  'offered_fixed': '821 3485 3569',
  'offered_fixed_presencial': '122', #348 is Unicesumar, #112 is Uniprojeção
  'fixed_99': '0 9',
  'fixed_198_semi': '0',  #2978 ... 2902 is Aqui Voce Pode (After subsidy 198 turns to 99)
  'fixed_99_ead': '1337',
  'pass_along': '2892 2901 1461 32 1422 1507 1611 56 562', 
  'pass_along_presencial_semi': '1638 1097', 
  'pass_along_ead': '0',
  'pass_along_semi':'0', 
  'pass_along_pos':'2265 283 735', #pós
  'pef_old': '3272 2996',
  'fixed_336': '0 3272',
  'fixed_139': '0 2985', #2985 is ESPG
  'fixed_f_o':'0 299 2378',
  'offered_limit': '0', #43 ... 1791 is Groupo Ser (17)
  'average_offered_full_fixed': '528',
  'average_offered_full_fixed_ead': '122',
  'average_offered_full_fixed_semi': '0',
  'average_offered_full_fixed_qap': '692', # FAC (Grupo Nilton Lins)
  'full_fixed_qap': '0', # FCV
  'full_fixed_qap_pos': '0',
  'offered_one_half': '0',
  'fake_admission': '562 1422 1420 56 48 3421 32 1461 543 2327 1986',
  'pass_along_campuses': '0 55703'
}

GLOBAL_PARAMS = {
  'min': 149.9,
  'min_7_plus':189.90,
  'min_12': 99.9,
  'min_qap_1': 99.9,
  'min_qap_7': 139.9,
  'min_qap_12':79.9,
  'min_qap_7_lowest':79.9
}

CITY_WEIGHTS_LEVEL_1_KIND_1 = {
  8473: 0.85, # Recife
  8841: 0.90, # Curitiba
  9214: 0.90, # Rio de Janeiro
  # 7778: campo grande
  8212: 0.80, # João Pessoa
  7993: 0.95, # Belém
  6098: 0.90, # Salvador
  6238: 0.90, # Fortaleza
  5722: 1.10, # Manaus
  9457: 1.20, # Porto Velho
  8385: 0.50, # Caruaru
  5893: 0.70, # Feira de Santana
  8739: 0.90, # Teresina
  10445: 0.85, # Campinas
  10726: 0.80, # Osasco
  10896: 0.90, # São José dos Campos
  10920: 0.70, # Sorocaba
  10873: 1.10, # Santo André
  6278: 0.90, # Juazeiro do Norte
  7874: 0.80, # Cuiabá
  9408: 0.80 # Boa Vista

  # Palmas: 0.90
  # Maceio: 0.90
  # Cuiabá: 0.80
  # Boa Vista: 0.90
  # São José:
  # Macapá: 0.80
  # Campo Grande: 0.75
  # Rio Branco: 0.75




}

# city_id city  state revenue_city
# 10901 São Paulo SP  4946554.95
# 9214  Rio de Janeiro  RJ  1377071.69
# 8841  Curitiba  PR  1301467.31
# 6364  Brasília  DF  946222.71
# 5722  Manaus  AM  748079.72
# 6971  Belo Horizonte  MG  633577.37
# 6238  Fortaleza CE  607482.87
# 10896 São José dos Campos SP  464326.95
# 6098  Salvador  BA  307033.12
# 10920 Sorocaba  SP  273615.91
# 10895 São José do Rio Preto SP  248755.68
# 8473  Recife  PE  247135.20
# 8739  Teresina  PI  169354.20
# 7993  Belém PA  148232.38
# 6442  Vitória ES  133350.54
# 9326  Natal RN  119857.39
# 10726 Osasco  SP  116587.73
# 7730  Uberlândia  MG  110599.53
# 9196  Nova Iguaçu RJ  100136.72
# 8939  Londrina  PR  99787.84
# 10264 Aracaju SE  93467.15
# 5753  Macapá  AP  33827.80

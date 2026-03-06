#!/usr/bin/env python3
"""seed_data.py

Popula o banco retail_analytics com dados realistas de uma marca brasileira
de cosmeticos (YMED). Gera ~750.000 registros em ~3-5 minutos.

Uso:
  python3 seed_data.py            # trunca e re-insere (pede confirmacao)
  python3 seed_data.py --force    # sem confirmacao
"""

from __future__ import annotations

import hashlib
import random
import sys
import time
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

# ---------------------------------------------------------------------------
# Conexao - tenta mysql.connector, fallback pymysql
# ---------------------------------------------------------------------------
try:
    import mysql.connector as db_connector

    def _connect(cfg):
        return db_connector.connect(
            host=cfg["host"], port=int(cfg["port"]),
            user=cfg["user"], password=cfg["password"],
            database=cfg["database"],
            connection_timeout=300,
            autocommit=False,
        )
except ImportError:
    import pymysql as db_connector  # type: ignore

    def _connect(cfg):
        return db_connector.connect(
            host=cfg["host"], port=int(cfg["port"]),
            user=cfg["user"], password=cfg["password"],
            database=cfg["database"],
            connect_timeout=300,
            autocommit=False,
        )

PROJECT_DIR = Path(__file__).resolve().parent
BATCH_SIZE = 2000

# ===================================================================
# CONSTANTES CURADAS
# ===================================================================

FIRST_NAMES_F = [
    "Ana", "Maria", "Juliana", "Fernanda", "Camila", "Beatriz", "Larissa",
    "Mariana", "Carolina", "Amanda", "Gabriela", "Leticia", "Patricia",
    "Raquel", "Vanessa", "Bruna", "Isabela", "Natalia", "Aline", "Tatiana",
    "Luciana", "Renata", "Daniela", "Priscila", "Rafaela", "Monica",
    "Simone", "Claudia", "Adriana", "Sandra", "Carla", "Cristina",
    "Elaine", "Viviane", "Fabiana", "Michele", "Thaisa", "Luana",
    "Bianca", "Debora", "Flavia", "Helena", "Ingrid", "Jessica",
    "Karen", "Livia", "Manuela", "Nathalia", "Olivia", "Paula",
]

FIRST_NAMES_M = [
    "Lucas", "Pedro", "Rafael", "Bruno", "Gustavo", "Felipe", "Thiago",
    "Rodrigo", "Leonardo", "Marcelo", "Andre", "Carlos", "Diego",
    "Eduardo", "Fernando", "Gabriel", "Henrique", "Igor", "Joao",
    "Leandro", "Matheus", "Nicolas", "Otavio", "Paulo", "Ricardo",
]

LAST_NAMES = [
    "Silva", "Santos", "Oliveira", "Souza", "Pereira", "Costa", "Rodrigues",
    "Almeida", "Nascimento", "Lima", "Araujo", "Fernandes", "Carvalho",
    "Gomes", "Martins", "Rocha", "Ribeiro", "Alves", "Monteiro", "Mendes",
    "Barros", "Freitas", "Barbosa", "Pinto", "Moura", "Cavalcanti",
    "Dias", "Castro", "Campos", "Cardoso", "Teixeira", "Vieira",
    "Moreira", "Nunes", "Ramos", "Lopes", "Correia", "Batista",
    "Rezende", "Melo", "Azevedo", "Farias", "Machado", "Pires",
    "Duarte", "Cunha", "Fonseca", "Sampaio", "Siqueira", "Brito",
]

# (cidade, UF, regiao, peso populacao)
LOCATIONS = [
    ("Sao Paulo", "SP", "Sudeste", 30),
    ("Rio de Janeiro", "RJ", "Sudeste", 18),
    ("Belo Horizonte", "MG", "Sudeste", 10),
    ("Campinas", "SP", "Sudeste", 5),
    ("Curitiba", "PR", "Sul", 6),
    ("Porto Alegre", "RS", "Sul", 5),
    ("Florianopolis", "SC", "Sul", 3),
    ("Salvador", "BA", "Nordeste", 6),
    ("Recife", "PE", "Nordeste", 5),
    ("Fortaleza", "CE", "Nordeste", 5),
    ("Brasilia", "DF", "Centro-Oeste", 5),
    ("Goiania", "GO", "Centro-Oeste", 3),
    ("Manaus", "AM", "Norte", 3),
    ("Belem", "PA", "Norte", 2),
    ("Vitoria", "ES", "Sudeste", 2),
    ("Niteroi", "RJ", "Sudeste", 2),
    ("Ribeirao Preto", "SP", "Sudeste", 2),
    ("Santos", "SP", "Sudeste", 2),
    ("Joinville", "SC", "Sul", 1),
    ("Londrina", "PR", "Sul", 1),
    ("Natal", "RN", "Nordeste", 2),
    ("Maceio", "AL", "Nordeste", 1),
    ("Joao Pessoa", "PB", "Nordeste", 1),
    ("Teresina", "PI", "Nordeste", 1),
    ("Sao Luis", "MA", "Nordeste", 1),
    ("Campo Grande", "MS", "Centro-Oeste", 1),
    ("Cuiaba", "MT", "Centro-Oeste", 1),
    ("Aracaju", "SE", "Nordeste", 1),
    ("Porto Velho", "RO", "Norte", 1),
    ("Macapa", "AP", "Norte", 1),
]

LOCATION_CITIES = [l[0] for l in LOCATIONS]
LOCATION_WEIGHTS = [l[3] for l in LOCATIONS]
LOCATION_MAP = {l[0]: (l[1], l[2]) for l in LOCATIONS}

INCOME_RANGES = ["Ate R$2.000", "R$2.001-R$5.000", "R$5.001-R$10.000",
                 "R$10.001-R$20.000", "Acima de R$20.000"]
INCOME_WEIGHTS = [15, 35, 30, 15, 5]

LOYALTY_TIERS = ["Bronze", "Prata", "Ouro", "Platina", "Diamante"]
LOYALTY_WEIGHTS = [50, 25, 15, 7, 3]

# --- Produtos ----
PRODUCT_CATALOG = {
    "Skincare": {
        "code": "SK",
        "count": 50,
        "subcategories": ["Limpeza Facial", "Hidratante Facial", "Serum", "Protetor Solar",
                          "Anti-idade", "Mascara Facial", "Tonico", "Esfoliante Facial"],
        "adjectives": ["Revitalizante", "Luminoso", "Hidra Boost", "Detox",
                       "Vitamina C", "Acido Hialuronico", "Retinol", "Niacinamida",
                       "Colageno", "Pepino"],
        "sizes": [30, 50, 60, 100, 120, 150, 200],
        "price_min": 29.90, "price_max": 189.90,
        "skin_type": True, "hair_type": False, "scent": False,
        "package_types": ["Tubo", "Frasco", "Pote", "Pump", "Bisnaga"],
    },
    "Haircare": {
        "code": "HC",
        "count": 45,
        "subcategories": ["Shampoo", "Condicionador", "Mascara Capilar",
                          "Leave-in", "Oleo Capilar", "Finalizador", "Tratamento Capilar"],
        "adjectives": ["Reconstrucao", "Hidratacao Profunda", "Brilho Intenso",
                       "Anti-Frizz", "Cachos Definidos", "Liso Perfeito", "Crescimento",
                       "Fortalecimento", "Nutricao"],
        "sizes": [200, 250, 300, 500, 1000],
        "price_min": 19.90, "price_max": 89.90,
        "skin_type": False, "hair_type": True, "scent": False,
        "package_types": ["Frasco", "Pote", "Tubo", "Pump"],
    },
    "Fragrance": {
        "code": "FR",
        "count": 30,
        "subcategories": ["Eau de Parfum", "Body Splash", "Eau de Toilette"],
        "adjectives": ["Floral", "Oriental", "Amadeirado", "Citrico", "Fresh",
                       "Noturno", "Elegance", "Tropical", "Seducao", "Intenso"],
        "sizes": [30, 50, 75, 100, 150],
        "price_min": 49.90, "price_max": 299.90,
        "skin_type": False, "hair_type": False, "scent": True,
        "package_types": ["Frasco", "Spray"],
    },
    "Makeup": {
        "code": "MK",
        "count": 40,
        "subcategories": ["Base", "Corretivo", "Po Compacto", "Batom",
                          "Rimel", "Sombra", "Blush", "Iluminador", "Primer"],
        "adjectives": ["HD", "Matte", "Glow", "Long Lasting", "Natural",
                       "Full Coverage", "Nude", "Intense", "Velvet"],
        "sizes": [5, 10, 15, 30, 40],
        "price_min": 24.90, "price_max": 149.90,
        "skin_type": True, "hair_type": False, "scent": False,
        "package_types": ["Tubo", "Frasco", "Pote", "Bisnaga"],
    },
    "Body Care": {
        "code": "BC",
        "count": 35,
        "subcategories": ["Hidratante Corporal", "Oleo Corporal", "Esfoliante Corporal",
                          "Sabonete Liquido", "Desodorante", "Creme para Maos"],
        "adjectives": ["Nutritivo", "Firmador", "Relaxante", "Energizante",
                       "Manteiga de Karite", "Oleo de Coco", "Aloe Vera",
                       "Lavanda", "Vanilla"],
        "sizes": [100, 200, 250, 400, 500],
        "price_min": 14.90, "price_max": 69.90,
        "skin_type": False, "hair_type": False, "scent": True,
        "package_types": ["Frasco", "Pote", "Pump", "Tubo", "Spray"],
    },
}

HAIR_TYPES = ["Liso", "Ondulado", "Cacheado", "Crespo", "Todos"]
SKIN_TYPES = ["Normal", "Seca", "Oleosa", "Mista", "Sensivel", "Todos"]
SCENT_PROFILES = ["Floral", "Amadeirado", "Citrico", "Oriental", "Frutal", "Herbal"]
CLAIM_OPTIONS = ["Vegano", "Cruelty-Free", "Organico", "Dermatologicamente Testado",
                 "Hipoalergenico", "Sem Parabenos", "Sem Sulfato"]
TARGET_AUDIENCES = ["Feminino", "Masculino", "Unissex"]
TARGET_WEIGHTS = [60, 10, 30]

# --- Lojas ---
STORE_DEFS = [
    ("Loja Propria", 20),
    ("Farmacia", 10),
    ("Departamento", 5),
    ("E-commerce Proprio", 3),
    ("Marketplace", 7),
    ("Franquia", 5),
]

MARKETPLACE_NAMES = ["Mercado Livre", "Amazon BR", "Magazine Luiza",
                     "Shopee", "Americanas", "Beleza na Web", "Epoca Cosmeticos"]

# --- Orders ---
PAYMENT_METHODS = ["Cartao de Credito", "PIX", "Boleto", "Cartao de Debito", "Vale Presente"]
PAYMENT_WEIGHTS = [45, 30, 15, 7, 3]

SHIPPING_TYPES_ONLINE = ["PAC", "SEDEX", "Expresso", "Retirada em Loja"]
SHIPPING_WEIGHTS_ONLINE = [35, 30, 25, 10]

ORDER_STATUSES = ["Entregue", "Concluido", "Cancelado", "Em Transito", "Devolvido"]
ORDER_STATUS_WEIGHTS = [55, 25, 8, 7, 5]

COUPONS = ["YMED10", "PRIMEIRACOMPRA", "BLACKFRIDAY", "FRETEGRATIS",
           "DIADASMAES", "VERAO25", "NATAL20", "ANIVERSARIO15", "INDICACAO"]

# Sazonalidade mensal (multiplicador do volume de pedidos)
MONTHLY_MULT = {
    1: 0.8, 2: 0.7, 3: 0.85, 4: 0.9, 5: 1.3,   # Maio = Dia das Maes
    6: 0.9, 7: 0.8, 8: 0.85, 9: 0.9, 10: 1.0,
    11: 1.5, 12: 1.6,  # Black Friday + Natal
}

# --- Reviews ---
REVIEW_TITLES_POS = [
    "Amei!", "Produto incrivel", "Super recomendo", "Melhor compra do ano",
    "Maravilhoso", "Excelente custo-beneficio", "Virou meu favorito",
    "Sensacional", "Nota 10", "Otimo produto",
]
REVIEW_TITLES_NEU = [
    "Razoavel", "Ok, nada demais", "Cumpre o basico", "Esperava mais",
    "Bom mas poderia melhorar", "Regular",
]
REVIEW_TITLES_NEG = [
    "Decepcionada", "Nao recomendo", "Esperava mais", "Produto ruim",
    "Nao funcionou para mim", "Arrependida da compra", "Horrivel",
]

POSITIVE_DETAILS = [
    "Minha pele ficou muito mais hidratada e macia.",
    "O aroma e suave e muito agradavel, nao incomoda.",
    "A textura e leve e absorve muito rapido na pele.",
    "Notei diferenca logo na primeira semana de uso.",
    "Meu cabelo ficou super macio e brilhoso depois de usar.",
    "Rende bastante, uso pouca quantidade cada vez.",
    "Nao irrita minha pele que e super sensivel.",
    "Produto de qualidade profissional por um preco acessivel.",
]

NEUTRAL_DETAILS = [
    "O produto e ok, faz o que promete mas nada excepcional.",
    "Para o preco eu esperava um resultado um pouco melhor.",
    "Nao e ruim mas existem opcoes melhores no mercado.",
    "Cumpre o basico mas nao me surpreendeu em nada.",
]

NEGATIVE_DETAILS = [
    "Causou irritacao e vermelhidao na minha pele.",
    "O cheiro e muito forte e artificial, nao consegui usar.",
    "Nao vi nenhum resultado apos usar o pote inteiro.",
    "Textura muito oleosa e pesada, deixa a pele grudenta.",
    "A cor e completamente diferente do que mostra no site.",
    "Produto com cheiro de vencido, parecia estragado.",
]

REPEAT_PHRASES = [
    "Com certeza comprarei de novo!",
    "Produto que nao pode faltar na minha rotina.",
    "Vale muito a pena investir.",
]

# --- Complaints ---
COMPLAINT_TYPES = [
    "Produto com Defeito", "Alergia/Reacao", "Produto Diferente do Anunciado",
    "Atraso na Entrega", "Embalagem Danificada", "Cobranca Indevida",
    "Atendimento Insatisfatorio", "Produto Vencido", "Troca/Devolucao",
]
COMPLAINT_TYPE_WEIGHTS = [20, 10, 15, 20, 10, 5, 5, 5, 10]

COMPLAINT_CHANNELS = ["SAC Telefone", "Email", "Chat Online", "Redes Sociais",
                      "Reclame Aqui", "Procon", "Loja Fisica"]
COMPLAINT_CHANNEL_WEIGHTS = [20, 25, 20, 15, 10, 3, 7]

SEVERITIES = ["Baixa", "Media", "Alta", "Critica"]
SEVERITY_WEIGHTS = [30, 40, 20, 10]

COMPLAINT_STATUSES = ["Aberto", "Em Andamento", "Resolvido", "Fechado"]

COMPLAINT_TEXTS = {
    "Produto com Defeito": [
        "Comprei o {product} e ao abrir percebi que a tampa estava quebrada. Gostaria de solicitar a troca do produto.",
        "O {product} veio com a embalagem amassada e o produto vazando. Peco reembolso ou reenvio.",
    ],
    "Alergia/Reacao": [
        "Apos usar o {product}, tive uma reacao alergica com coceira e vermelhidao. Preciso de orientacao e reembolso.",
        "O {product} causou irritacao intensa no meu rosto. Minha pele ficou vermelha e ardendo por horas.",
    ],
    "Produto Diferente do Anunciado": [
        "O {product} que recebi e completamente diferente das fotos do site. A cor e a textura nao correspondem.",
        "O {product} promete resultado em 7 dias mas ja uso ha 1 mes sem nenhuma mudanca. Propaganda enganosa.",
    ],
    "Atraso na Entrega": [
        "Fiz o pedido #{order_id} ha {days} dias e ate agora nao recebi. O rastreamento nao atualiza.",
        "Meu pedido #{order_id} esta com {days} dias de atraso. Preciso do produto urgentemente pois e um presente.",
    ],
    "Embalagem Danificada": [
        "O {product} chegou com a caixa toda amassada e o frasco trincado. O produto esta inutilizavel.",
        "Recebi o pedido e a embalagem estava aberta. Nao tenho como saber se o produto foi adulterado.",
    ],
    "Cobranca Indevida": [
        "Fui cobrada duas vezes pelo pedido #{order_id}. Preciso do estorno imediato de uma das cobranças.",
        "Cancelei o pedido mas o valor ainda nao foi estornado no meu cartao. Ja fazem {days} dias uteis.",
    ],
    "Atendimento Insatisfatorio": [
        "Tentei resolver meu problema pelo SAC e fui mal atendida. O atendente foi grosseiro e nao resolveu nada.",
        "Liguei 3 vezes para o SAC sobre o {product} e cada vez me deram uma informacao diferente.",
    ],
    "Produto Vencido": [
        "Recebi o {product} e ao verificar a validade percebi que vence no proximo mes. Produto praticamente vencido.",
        "O {product} que comprei esta com a data de validade apagada. Nao tenho como saber se esta vencido.",
    ],
    "Troca/Devolucao": [
        "Gostaria de trocar o {product} por outro tom. Como faco para enviar de volta?",
        "Comprei o {product} mas nao era o que eu esperava. Quero devolver e receber o reembolso.",
    ],
}

# --- Returns ---
RETURN_REASONS = [
    "Produto com Defeito", "Tamanho/Volume Errado", "Nao Gostei",
    "Produto Diferente do Anunciado", "Alergia/Reacao", "Arrependimento",
    "Presente Duplicado", "Embalagem Danificada",
]
RETURN_REASON_WEIGHTS = [25, 10, 20, 15, 8, 12, 5, 5]


# ===================================================================
# FUNCOES AUXILIARES
# ===================================================================

def load_env(filepath: Path) -> dict[str, str]:
    """Parse manual do .env."""
    env: dict[str, str] = {}
    if not filepath.exists():
        return env
    for line in filepath.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def dec(value: float) -> Decimal:
    """Converte float para Decimal com 2 casas."""
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, max(delta, 0)))


def rand_datetime(start: date, end: date) -> datetime:
    d = rand_date(start, end)
    return datetime(d.year, d.month, d.day,
                    random.randint(6, 23), random.randint(0, 59), random.randint(0, 59))


# ===================================================================
# GERADORES DE DADOS
# ===================================================================

class DataGenerator:
    def __init__(self, seed: int = 42):
        random.seed(seed)
        self.products: list[tuple] = []
        self.customers: list[tuple] = []
        self.stores: list[tuple] = []
        self.orders: list[tuple] = []
        self.order_items: list[tuple] = []
        self.inventory_snapshots: list[tuple] = []
        self.reviews: list[tuple] = []
        self.complaints: list[tuple] = []
        self.returns: list[tuple] = []

        # Lookup internos
        self._product_prices: dict[int, float] = {}
        self._product_names: dict[int, str] = {}
        self._active_product_ids: list[int] = []
        self._customer_ids: list[int] = []
        self._store_ids: list[int] = []
        self._online_store_ids: list[int] = []
        self._physical_store_ids: list[int] = []
        self._completed_orders: list[tuple] = []  # (order_id, customer_id, order_datetime)
        self._order_product_map: dict[int, list[int]] = {}  # order_id -> [product_ids]

    # ---------- PRODUCTS ----------
    def generate_products(self) -> None:
        print("Gerando products...", flush=True)
        pid = 0
        for cat_name, cat in PRODUCT_CATALOG.items():
            for i in range(cat["count"]):
                pid += 1
                sub = random.choice(cat["subcategories"])
                adj = random.choice(cat["adjectives"])
                size = random.choice(cat["sizes"])
                sku = f"YMED-{cat['code']}-{pid:04d}"
                name = f"YMED {sub} {adj} {size}ml"
                brand = "YMED"
                audience = random.choices(TARGET_AUDIENCES, TARGET_WEIGHTS)[0]
                hair = random.choice(HAIR_TYPES) if cat["hair_type"] else None
                skin = random.choice(SKIN_TYPES) if cat["skin_type"] else None
                scent = random.choice(SCENT_PROFILES) if cat["scent"] else None
                claims = ", ".join(random.sample(CLAIM_OPTIONS, k=random.randint(2, 5)))
                pkg = random.choice(cat["package_types"])
                list_price = round(random.uniform(cat["price_min"], cat["price_max"]), 2)
                unit_cost = round(list_price * random.uniform(0.30, 0.50), 2)
                launch = rand_date(date(2019, 1, 1), date(2025, 6, 1))
                active = 1 if random.random() < 0.90 else 0

                self.products.append((
                    pid, sku, name, brand, cat_name, sub, audience,
                    hair, skin, scent, claims, size, pkg,
                    dec(unit_cost), dec(list_price), launch, active,
                ))
                self._product_prices[pid] = list_price
                self._product_names[pid] = name
                if active:
                    self._active_product_ids.append(pid)

        print(f"  -> {len(self.products)} products gerados", flush=True)

    # ---------- CUSTOMERS ----------
    def generate_customers(self, n: int = 10_000) -> None:
        print("Gerando customers...", flush=True)
        for cid in range(1, n + 1):
            if random.random() < 0.70:
                first = random.choice(FIRST_NAMES_F)
                gender = "Feminino"
            else:
                first = random.choice(FIRST_NAMES_M)
                gender = "Masculino"
            last = random.choice(LAST_NAMES)
            name = f"{first} {last}"
            email_hash = hashlib.sha256(f"{name}_{cid}".encode()).hexdigest()[:40]
            birth = rand_date(date(1960, 1, 1), date(2005, 12, 31))
            city = random.choices(LOCATION_CITIES, LOCATION_WEIGHTS)[0]
            state, region = LOCATION_MAP[city]
            income = random.choices(INCOME_RANGES, INCOME_WEIGHTS)[0]
            signup = rand_date(date(2021, 1, 1), date(2025, 12, 31))
            tier = random.choices(LOYALTY_TIERS, LOYALTY_WEIGHTS)[0]

            self.customers.append((
                cid, name, email_hash, gender, birth, city, state,
                "Brasil", income, signup, tier,
            ))
            self._customer_ids.append(cid)

        print(f"  -> {len(self.customers)} customers gerados", flush=True)

    # ---------- STORES ----------
    def generate_stores(self) -> None:
        print("Gerando stores...", flush=True)
        sid = 0
        mkt_idx = 0
        for channel, count in STORE_DEFS:
            for i in range(count):
                sid += 1
                if channel in ("E-commerce Proprio", "Marketplace"):
                    if channel == "Marketplace":
                        mkt_name = MARKETPLACE_NAMES[mkt_idx % len(MARKETPLACE_NAMES)]
                        mkt_idx += 1
                        sname = f"YMED - {mkt_name}"
                    else:
                        sname = f"YMED Online {i + 1}"
                    city, state, region = "Online", "BR", "Nacional"
                    self._online_store_ids.append(sid)
                else:
                    city = random.choices(LOCATION_CITIES, LOCATION_WEIGHTS)[0]
                    state, region = LOCATION_MAP[city]
                    sname = f"YMED {channel} {city} {i + 1}"
                    self._physical_store_ids.append(sid)

                self.stores.append((
                    sid, sname, channel, city, state, "Brasil", region,
                ))
                self._store_ids.append(sid)

        print(f"  -> {len(self.stores)} stores gerados", flush=True)

    # ---------- ORDERS + ORDER_ITEMS ----------
    def generate_orders(self, n: int = 100_000) -> None:
        print("Gerando orders e order_items...", flush=True)
        start_date = date(2024, 1, 1)
        end_date = date(2025, 12, 31)

        # Distribui pedidos por mês considerando sazonalidade
        total_mult = sum(MONTHLY_MULT[m] for m in range(1, 13)) * 2  # 2 anos
        months: list[tuple[int, int]] = []
        for year in (2024, 2025):
            for month in range(1, 13):
                months.append((year, month))

        orders_per_month: list[int] = []
        for year, month in months:
            frac = MONTHLY_MULT[month] / total_mult
            orders_per_month.append(max(1, int(n * frac)))

        # Ajuste para bater o total
        diff = n - sum(orders_per_month)
        for i in range(abs(diff)):
            idx = i % len(orders_per_month)
            orders_per_month[idx] += 1 if diff > 0 else -1

        oid = 0
        oiid = 0
        items_per_order_opts = [1, 2, 3, 4, 5]
        items_per_order_weights = [30, 30, 20, 12, 8]

        for mi, (year, month) in enumerate(months):
            n_orders = orders_per_month[mi]
            # Dias do mes
            if month == 12:
                last_day = 31
            else:
                next_m = date(year, month + 1, 1)
                last_day = (next_m - timedelta(days=1)).day

            for _ in range(n_orders):
                oid += 1
                day = random.randint(1, last_day)
                hour = random.randint(6, 23)
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                order_dt = datetime(year, month, day, hour, minute, second)

                cust_id = random.choice(self._customer_ids)

                # 60% online, 40% fisico
                if random.random() < 0.60 and self._online_store_ids:
                    store_id = random.choice(self._online_store_ids)
                    is_online = True
                else:
                    store_id = random.choice(self._physical_store_ids) if self._physical_store_ids else random.choice(self._store_ids)
                    is_online = False

                payment = random.choices(PAYMENT_METHODS, PAYMENT_WEIGHTS)[0]

                if is_online:
                    shipping = random.choices(SHIPPING_TYPES_ONLINE, SHIPPING_WEIGHTS_ONLINE)[0]
                else:
                    shipping = "Retirada em Loja"

                coupon = None
                has_coupon = random.random() < 0.15
                if has_coupon:
                    coupon = random.choice(COUPONS)

                status = random.choices(ORDER_STATUSES, ORDER_STATUS_WEIGHTS)[0]

                # Gerar itens
                n_items = random.choices(items_per_order_opts, items_per_order_weights)[0]
                chosen_products = random.sample(
                    self._active_product_ids,
                    k=min(n_items, len(self._active_product_ids))
                )

                gross = Decimal("0.00")
                order_item_list: list[tuple] = []
                order_product_ids: list[int] = []

                for prod_id in chosen_products:
                    oiid += 1
                    qty = random.choices([1, 2, 3], [70, 20, 10])[0]
                    base_price = self._product_prices[prod_id]
                    # Pequena variacao de preco (+/- 5%)
                    unit_price = dec(base_price * random.uniform(0.95, 1.05))
                    item_discount = dec(0)
                    if has_coupon and random.random() < 0.5:
                        item_discount = dec(float(unit_price) * qty * random.uniform(0.05, 0.15))
                    net_line = dec(float(unit_price) * qty - float(item_discount))
                    gross += dec(float(unit_price) * qty)

                    order_item_list.append((
                        oiid, oid, prod_id, qty, unit_price, item_discount, net_line,
                    ))
                    order_product_ids.append(prod_id)

                # Amounts do pedido
                if has_coupon:
                    discount_pct = random.uniform(0.05, 0.25)
                else:
                    discount_pct = random.uniform(0.00, 0.03)
                discount_amt = dec(float(gross) * discount_pct)
                tax_amt = dec(float(gross) * random.uniform(0.08, 0.12))

                if is_online and shipping != "Retirada em Loja":
                    if coupon == "FRETEGRATIS":
                        ship_amt = dec(0)
                    else:
                        ship_amt = dec(random.uniform(9.90, 39.90))
                else:
                    ship_amt = dec(0)

                net_amt = gross - discount_amt + tax_amt + ship_amt

                self.orders.append((
                    oid, cust_id, store_id, order_dt, payment, shipping,
                    coupon, status, gross, discount_amt, tax_amt, ship_amt, net_amt,
                ))
                self.order_items.extend(order_item_list)
                self._order_product_map[oid] = order_product_ids

                if status in ("Entregue", "Concluido"):
                    self._completed_orders.append((oid, cust_id, order_dt))

        print(f"  -> {len(self.orders)} orders gerados", flush=True)
        print(f"  -> {len(self.order_items)} order_items gerados", flush=True)

    # ---------- INVENTORY SNAPSHOTS ----------
    def generate_inventory(self) -> None:
        print("Gerando inventory_snapshots...", flush=True)
        # Snapshots semanais (toda segunda) por 2 anos
        start = date(2024, 1, 1)
        end = date(2025, 12, 31)
        current = start
        # Avanca ate a proxima segunda
        while current.weekday() != 0:
            current += timedelta(days=1)

        snapshot_dates: list[date] = []
        while current <= end:
            snapshot_dates.append(current)
            current += timedelta(days=7)

        # Para cada snapshot, amostra 70% dos produtos ativos em cada loja
        sample_size = max(1, int(len(self._active_product_ids) * 0.70))
        physical_stores = self._physical_store_ids if self._physical_store_ids else self._store_ids[:30]

        for snap_date in snapshot_dates:
            for store_id in physical_stores:
                products_sample = random.sample(self._active_product_ids, k=min(sample_size, len(self._active_product_ids)))
                for prod_id in products_sample:
                    stock = random.randint(0, 500)
                    reserved = random.randint(0, min(stock, 50))
                    transit = random.randint(0, 200)
                    reorder = random.choice([20, 30, 50, 75, 100])

                    self.inventory_snapshots.append((
                        snap_date, store_id, prod_id, stock, reserved, transit, reorder,
                    ))

        print(f"  -> {len(self.inventory_snapshots)} inventory_snapshots gerados", flush=True)

    # ---------- REVIEWS ----------
    def generate_reviews(self, n: int = 15_000) -> None:
        print("Gerando reviews...", flush=True)
        seen: set[tuple[int, int]] = set()  # (customer_id, product_id)
        rid = 0
        rating_opts = [5, 4, 3, 2, 1]
        rating_weights = [40, 25, 15, 12, 8]

        attempts = 0
        while rid < n and attempts < n * 3:
            attempts += 1
            if not self._completed_orders:
                break
            order_id, cust_id, order_dt = random.choice(self._completed_orders)
            prods = self._order_product_map.get(order_id, [])
            if not prods:
                continue
            prod_id = random.choice(prods)
            key = (cust_id, prod_id)
            if key in seen:
                continue
            seen.add(key)
            rid += 1

            rating = random.choices(rating_opts, rating_weights)[0]
            review_dt = order_dt + timedelta(days=random.randint(3, 45))
            prod_name = self._product_names.get(prod_id, "produto")

            if rating >= 4:
                title = random.choice(REVIEW_TITLES_POS)
                detail = random.choice(POSITIVE_DETAILS)
                extra = f" {random.choice(REPEAT_PHRASES)}" if random.random() < 0.4 else ""
                text = f"{detail}{extra}"
            elif rating == 3:
                title = random.choice(REVIEW_TITLES_NEU)
                text = random.choice(NEUTRAL_DETAILS)
            else:
                title = random.choice(REVIEW_TITLES_NEG)
                text = random.choice(NEGATIVE_DETAILS)

            self.reviews.append((
                rid, cust_id, prod_id, review_dt, rating, title, text,
            ))

        print(f"  -> {len(self.reviews)} reviews gerados", flush=True)

    # ---------- COMPLAINTS ----------
    def generate_complaints(self, n: int = 5_000) -> None:
        print("Gerando complaints...", flush=True)
        cid = 0
        for i in range(n):
            cid += 1
            comp_type = random.choices(COMPLAINT_TYPES, COMPLAINT_TYPE_WEIGHTS)[0]
            channel = random.choices(COMPLAINT_CHANNELS, COMPLAINT_CHANNEL_WEIGHTS)[0]
            severity = random.choices(SEVERITIES, SEVERITY_WEIGHTS)[0]

            # 70% vinculada a pedido, 30% sem pedido
            if random.random() < 0.70 and self._completed_orders:
                order_id, cust_id_ref, order_dt = random.choice(self._completed_orders)
                prods = self._order_product_map.get(order_id, self._active_product_ids[:1])
                prod_id = random.choice(prods) if prods else random.choice(self._active_product_ids)
                comp_dt = order_dt + timedelta(days=random.randint(1, 60))
                # 10% anonimo
                if random.random() < 0.10:
                    cust_id_ref = None
            else:
                order_id = None
                cust_id_ref = random.choice(self._customer_ids) if random.random() > 0.10 else None
                prod_id = random.choice(self._active_product_ids)
                comp_dt = rand_datetime(date(2024, 1, 1), date(2025, 12, 31))

            prod_name = self._product_names.get(prod_id, "produto")
            templates = COMPLAINT_TEXTS.get(comp_type, COMPLAINT_TEXTS["Produto com Defeito"])
            text = random.choice(templates)
            text = text.replace("{product}", prod_name)
            text = text.replace("{order_id}", str(order_id or "00000"))
            text = text.replace("{days}", str(random.randint(5, 30)))

            # Status baseado na "idade"
            age_days = (datetime(2026, 1, 1) - comp_dt).days
            if age_days > 60:
                status = random.choices(["Resolvido", "Fechado"], [60, 40])[0]
            elif age_days > 14:
                status = random.choices(["Em Andamento", "Resolvido", "Fechado"], [30, 50, 20])[0]
            else:
                status = random.choices(["Aberto", "Em Andamento"], [60, 40])[0]

            resolution = None
            if status in ("Resolvido", "Fechado"):
                resolution = random.randint(1, 30)

            self.complaints.append((
                cid, cust_id_ref, order_id, prod_id, comp_dt, channel,
                comp_type, text, severity, status, resolution,
            ))

        print(f"  -> {len(self.complaints)} complaints gerados", flush=True)

    # ---------- RETURNS ----------
    def generate_returns(self, n: int = 8_000) -> None:
        print("Gerando returns...", flush=True)
        rid = 0
        attempts = 0
        while rid < n and attempts < n * 3:
            attempts += 1
            if not self._completed_orders:
                break
            order_id, cust_id, order_dt = random.choice(self._completed_orders)
            prods = self._order_product_map.get(order_id, [])
            if not prods:
                continue
            prod_id = random.choice(prods)
            rid += 1

            return_dt = order_dt + timedelta(days=random.randint(5, 30))
            qty = random.choices([1, 2, 3], [80, 15, 5])[0]
            reason = random.choices(RETURN_REASONS, RETURN_REASON_WEIGHTS)[0]
            price = self._product_prices.get(prod_id, 50.0)
            refund = dec(price * qty)

            self.returns.append((
                rid, order_id, prod_id, return_dt, qty, reason, refund,
            ))

        print(f"  -> {len(self.returns)} returns gerados", flush=True)

    # ---------- GERAR TUDO ----------
    def generate_all(self) -> None:
        self.generate_products()
        self.generate_customers()
        self.generate_stores()
        self.generate_orders()
        self.generate_inventory()
        self.generate_reviews()
        self.generate_complaints()
        self.generate_returns()


# ===================================================================
# DATABASE SEEDER
# ===================================================================

class DatabaseSeeder:
    def __init__(self, config: dict[str, str]):
        self.config = config
        self.conn = None
        self.cursor = None

    def connect(self) -> None:
        self.conn = _connect(self.config)
        self.cursor = self.conn.cursor()
        print(f"Conectado ao MySQL {self.config['host']}:{self.config['port']}/{self.config['database']}")

    def close(self) -> None:
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def has_data(self) -> bool:
        self.cursor.execute("SELECT COUNT(*) FROM orders")
        row = self.cursor.fetchone()
        return row is not None and row[0] > 0

    def truncate_all(self) -> None:
        print("Truncando todas as tabelas...", flush=True)
        tables = [
            "text_embeddings", "returns", "complaints", "reviews",
            "inventory_snapshots", "order_items", "orders",
            "products", "customers", "stores",
        ]
        self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        for t in tables:
            self.cursor.execute(f"TRUNCATE TABLE {t}")
        self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        self.conn.commit()
        print("  -> Todas as tabelas truncadas", flush=True)

    def batch_insert(self, table: str, columns: list[str], rows: list[tuple]) -> None:
        if not rows:
            return
        placeholders = ", ".join(["%s"] * len(columns))
        col_str = ", ".join(columns)
        sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"

        total = len(rows)
        start = time.time()
        for i in range(0, total, BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            self.cursor.executemany(sql, batch)
            self.conn.commit()
            done = min(i + BATCH_SIZE, total)
            pct = done / total * 100
            elapsed = time.time() - start
            print(f"  {table}: {done:>9,}/{total:,} ({pct:5.1f}%) [{elapsed:.1f}s]",
                  end="\r", flush=True)
        elapsed = time.time() - start
        print(f"  {table}: {total:>9,}/{total:,} (100.0%) [{elapsed:.1f}s]", flush=True)

    def seed_all(self, gen: DataGenerator) -> None:
        t0 = time.time()

        # Dimensoes
        print("\n=== Inserindo dimensoes ===")
        self.batch_insert("products", [
            "product_id", "sku", "product_name", "brand", "product_category",
            "product_subcategory", "target_audience", "hair_type", "skin_type",
            "scent_profile", "claim_tags", "package_size_ml_g", "package_type",
            "unit_cost", "list_price", "launch_date", "is_active",
        ], gen.products)

        self.batch_insert("customers", [
            "customer_id", "customer_name", "email_hash", "gender", "birth_date",
            "city", "state", "country", "income_range", "signup_date", "loyalty_tier",
        ], gen.customers)

        self.batch_insert("stores", [
            "store_id", "store_name", "channel", "city", "state", "country", "region",
        ], gen.stores)

        # Fatos
        print("\n=== Inserindo fatos ===")
        self.batch_insert("orders", [
            "order_id", "customer_id", "store_id", "order_datetime", "payment_method",
            "shipping_type", "coupon_code", "order_status", "gross_amount",
            "discount_amount", "tax_amount", "shipping_amount", "net_amount",
        ], gen.orders)

        self.batch_insert("order_items", [
            "order_item_id", "order_id", "product_id", "quantity", "unit_price",
            "discount_amount", "net_line_amount",
        ], gen.order_items)

        self.batch_insert("inventory_snapshots", [
            "snapshot_date", "store_id", "product_id", "stock_on_hand",
            "stock_reserved", "stock_in_transit", "reorder_point",
        ], gen.inventory_snapshots)

        # Texto
        print("\n=== Inserindo dados textuais ===")
        self.batch_insert("reviews", [
            "review_id", "customer_id", "product_id", "review_datetime",
            "rating", "review_title", "review_text",
        ], gen.reviews)

        self.batch_insert("complaints", [
            "complaint_id", "customer_id", "order_id", "product_id",
            "complaint_datetime", "complaint_channel", "complaint_type",
            "complaint_text", "severity", "status", "resolution_time_days",
        ], gen.complaints)

        self.batch_insert("returns", [
            "return_id", "order_id", "product_id", "return_datetime",
            "quantity", "return_reason", "refund_amount",
        ], gen.returns)

        total_time = time.time() - t0
        total_rows = (
            len(gen.products) + len(gen.customers) + len(gen.stores) +
            len(gen.orders) + len(gen.order_items) + len(gen.inventory_snapshots) +
            len(gen.reviews) + len(gen.complaints) + len(gen.returns)
        )

        print(f"\n{'=' * 55}")
        print(f"  SEED COMPLETO!")
        print(f"{'=' * 55}")
        print(f"  Total de registros: {total_rows:,}")
        print(f"  Tempo total:        {total_time:.1f}s")
        print()
        print("  Resumo por tabela:")
        print(f"    products:            {len(gen.products):>9,}")
        print(f"    customers:           {len(gen.customers):>9,}")
        print(f"    stores:              {len(gen.stores):>9,}")
        print(f"    orders:              {len(gen.orders):>9,}")
        print(f"    order_items:         {len(gen.order_items):>9,}")
        print(f"    inventory_snapshots: {len(gen.inventory_snapshots):>9,}")
        print(f"    reviews:             {len(gen.reviews):>9,}")
        print(f"    complaints:          {len(gen.complaints):>9,}")
        print(f"    returns:             {len(gen.returns):>9,}")
        print()


# ===================================================================
# MAIN
# ===================================================================

def main() -> None:
    force = "--force" in sys.argv

    # 1. Carregar .env
    env_path = PROJECT_DIR / ".env"
    env = load_env(env_path)
    if not env:
        print(f"Arquivo .env nao encontrado em {env_path}")
        print("Execute setup_mysql_retail_analytics.py primeiro.")
        sys.exit(1)

    db_config = {
        "host": env.get("DB_HOST", "127.0.0.1"),
        "port": env.get("DB_PORT", "3307"),
        "user": env.get("MYSQL_USER", env.get("DB_USER", "root")),
        "password": env.get("MYSQL_PASSWORD", env.get("DB_PASSWORD", "")),
        "database": env.get("MYSQL_DATABASE", env.get("DB_NAME", "retail_analytics")),
    }

    # 2. Conectar
    seeder = DatabaseSeeder(db_config)
    try:
        seeder.connect()
    except Exception as e:
        print(f"Erro ao conectar: {e}")
        print("Verifique se o container MySQL esta rodando (docker compose up -d)")
        sys.exit(1)

    # 3. Verificar dados existentes
    if seeder.has_data() and not force:
        resp = input("O banco ja contem dados. Truncar e re-inserir? [y/N]: ")
        if resp.strip().lower() != "y":
            print("Abortado.")
            seeder.close()
            return

    # 4. Truncar
    seeder.truncate_all()

    # 5. Gerar dados
    print("\n=== Gerando dados ===")
    gen = DataGenerator(seed=42)
    gen.generate_all()

    # 6. Inserir
    seeder.seed_all(gen)

    seeder.close()


if __name__ == "__main__":
    main()

TABELAS_SICONV = [
    {
        "nome_tabela": "proposta",
        "nome_csv": "siconv_proposta.csv",
        "conflict_fields": ["id_proposta"],
        "primary_key": ["id_proposta"],
        "skip_rows": 1,
    },
    {
        "nome_tabela": "convenio",
        "nome_csv": "siconv_convenio.csv",
        "conflict_fields": ["nr_convenio"],
        "primary_key": ["nr_convenio"],
        "skip_rows": 0,
    },
]
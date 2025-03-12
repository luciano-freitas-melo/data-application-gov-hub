with

    siafi_data as (select * from {{ ref("contratos_estagios") }}),

    compras_gov_data as (select * from {{ ref("cronogramas_faturas_mensal") }})

select
    c.contrato_id,
    c.mes_ref,
    c.valor_cronograma as comprasgov_valor_cronograma,
    c.valor_faturas as comprasgov_valor_faturas,
    s.valor_empenhado as siafi_valor_empenhado,
    s.valor_liquidado as siafi_valor_liquidado,
    s.valor_pago as siafi_valor_pago
from compras_gov_data as c
left join
    siafi_data as s on c.contrato_id = s.contrato_id and c.mes_ref = s.mes_lancamento

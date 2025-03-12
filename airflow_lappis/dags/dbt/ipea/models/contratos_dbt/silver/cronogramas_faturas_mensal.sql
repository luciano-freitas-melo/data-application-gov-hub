with

    cronograma_agg as (
        select contrato_id, vencimento as mes_ref, sum(valor) as valor_cronograma
        from {{ ref("cronogramas") }}
        group by 1, 2
        order by contrato_id, vencimento
    ),

    faturas_agg as (
        select
            contrato_id,
            to_date(
                split_part(emissao::text, '-', 1)
                || '-'
                || split_part(emissao::text, '-', 2),
                'YYYY-MM'
            ) as mes_ref,
            sum(juros + multa + glosa + valorliquido) as valor_faturas
        from {{ ref("faturas") }}
        group by 1, 2
    ),

    joined_table as (
        select * from cronograma_agg left join faturas_agg using (contrato_id, mes_ref)
    )

--
select
    contrato_id::text,
    mes_ref,
    coalesce(valor_cronograma, 0) as valor_cronograma,
    coalesce(valor_faturas, 0) as valor_faturas
from joined_table
order by contrato_id, mes_ref

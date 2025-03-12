with

    ids_filtrados as (
        select contrato_id, ne, cnpj_cpf, processo, info_complementar
        from {{ ref("identificadores") }}
        where categoria not in ('Cessão')
    ),

    -- Join 1
    id_table_1 as (select distinct contrato_id, ne, cnpj_cpf from ids_filtrados),

    joined_table_1 as (
        select
            contrato_id,
            cnpj_cpf,
            ne,
            num_processo,
            info_complementar,
            mes_lancamento,
            valor_empenhado,
            valor_liquidado,
            valor_pago
        from {{ ref("estagios_mensal") }}
        left join id_table_1 using (ne, cnpj_cpf)
    ),

    -- Part 2
    empenhos_restantes_1 as (select * from joined_table_1 where contrato_id is null),

    id_table_2 as (
        select distinct contrato_id, cnpj_cpf, processo as num_processo
        from ids_filtrados l
        where
            not exists (
                select distinct contrato_id
                from joined_table_1 r
                where r.contrato_id = l.contrato_id
            )
    ),

    joined_table_2 as (
        select
            r.contrato_id,
            cnpj_cpf,
            ne,
            num_processo,
            info_complementar,
            mes_lancamento,
            valor_empenhado,
            valor_liquidado,
            valor_pago
        from empenhos_restantes_1 l
        left join id_table_2 r using (cnpj_cpf, num_processo)
    ),

    -- Part 3
    empenhos_restantes_2 as (select * from joined_table_2 where contrato_id is null),

    id_table_3 as (
        select distinct t0.contrato_id, t0.cnpj_cpf, t0.info_complementar
        from ids_filtrados t0
        left join id_table_1 t1 on t0.contrato_id = t1.contrato_id
        left join id_table_2 t2 on t0.contrato_id = t2.contrato_id
        where t1.contrato_id is null or t2.contrato_id is null

    ),

    joined_table_3 as (
        select
            r.contrato_id,
            cnpj_cpf,
            ne,
            num_processo,
            info_complementar,
            mes_lancamento,
            valor_empenhado,
            valor_liquidado,
            valor_pago
        from empenhos_restantes_2 l
        left join id_table_3 r using (cnpj_cpf, info_complementar)
    ),

    result_table as (
        select *
        from joined_table_1
        union
        select *
        from joined_table_2
        union
        select *
        from joined_table_3
    )

--
select contrato_id, mes_lancamento, valor_empenhado, valor_liquidado, valor_pago
from result_table
where contrato_id is not null
order by contrato_id, mes_lancamento

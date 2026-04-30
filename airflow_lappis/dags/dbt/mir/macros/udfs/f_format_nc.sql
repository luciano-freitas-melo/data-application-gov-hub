{% macro create_f_format_nc() %}
    create or replace function {{ target.schema }}.format_nc(in_text text)
    returns text
    as $$ 
    
    with 

    pre_process as (
        select
            left(in_text, 7) as prefix,
            right(in_text, 4) as posfix_text
    ),

    normalized as (
        select
            prefix,
            case
                when posfix_text ~ '^[0-9]{1,4}$' then posfix_text::numeric
                else null
            end as posfix
        from pre_process
    )
    
    select
        case
            when posfix is null then null
            else concat(prefix, to_char(posfix, 'FM00000'))
        end as result
    from normalized
    
    $$
    language sql
    ;
{% endmacro %}

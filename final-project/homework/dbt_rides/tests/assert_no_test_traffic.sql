-- Тестові поїздки прибрано ЦІЛКОМ: у Silver немає жодної події й жодної поїздки тестового райдера.
select ride_id from {{ ref('rides') }} where rider_id like 'test-%'
union all
select ride_id from {{ ref('events') }} where source = 'loadtest'

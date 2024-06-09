CREATE VIEW IF NOT EXISTS dwh.vw_results AS
SELECT *
FROM dwh.fct_classification
LEFT JOIN dwh.dim_season AS season ON season.id = season_id
LEFT JOIN dwh.dim_event AS event ON event.id = event_id
LEFT JOIN dwh.dim_category AS category ON category.id = category_id
LEFT JOIN dwh.dim_session AS session ON session.id = session_id
LEFT JOIN dwh.dim_rider AS rider ON rider.id = rider_id;
CREATE OR REPLACE VIEW wine_summary AS
SELECT
    type,
    ROUND(AVG(alcohol)::numeric,2) AS avg_alcohol,
    ROUND(AVG(ph)::numeric,2) AS avg_ph
FROM winequalityN
GROUP BY type;

CREATE VIEW quality_metrics AS
SELECT
    type,
    AVG(ph) avg_ph,
    AVG(density) avg_density
FROM winequality
GROUP BY type;
-- Anzahl Datensätze
SELECT COUNT(*) AS anzahl_datensaetze
FROM winequalityN;

-- Fehlende Alkoholwerte
SELECT COUNT(*) AS fehlende_alkoholwerte
FROM winequalityN
WHERE alcohol IS NULL;

-- Durchschnittlicher Alkoholgehalt
SELECT
    ROUND(AVG(alcohol)::numeric,2) AS durchschnitt_alkohol
FROM winequalityN;

-- Anzahl Weine pro Typ
SELECT
    type,
    COUNT(*) AS anzahl
FROM winequalityN
GROUP BY type;
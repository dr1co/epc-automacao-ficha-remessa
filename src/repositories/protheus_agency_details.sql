SELECT
	gi6.GI6_DESCRI AS agency_name,
	g6x.G6X_AGENCI AS agency_code,
	g6x.G6X_NUMFCH AS ticket_number,
	CASE
		WHEN gzg.GZG_TIPO = 1 THEN 'RECEITA'
		WHEN gzg.GZG_TIPO = 2 THEN 'DESPESA'
		ELSE 'ERRO'
	END AS transaction_type,
	gzg.GZG_DESCRI AS transaction_description,
	gzg.GZG_VALOR AS transaction_value
FROM G6X010 g6x
INNER JOIN GI6010 gi6 ON gi6.GI6_CODIGO = g6x.G6X_AGENCI
INNER JOIN GZG010 gzg
	ON gzg.GZG_AGENCI = g6x.G6X_AGENCI
	AND gzg.GZG_NUMFCH = g6x.G6X_NUMFCH
WHERE g6x.G6X_DTREME = '{emission_date}'
    AND g6x.G6X_AGENCI = '{agency_code}'
	AND gi6.GI6_FILIAL = '{associated_company}'
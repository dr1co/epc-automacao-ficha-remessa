SELECT
	gi6.GI6_FILIAL AS associated_company,
	gi6.GI6_DESCRI AS agency_name,
	g6x.G6X_AGENCI AS agency_code,
	g6x.G6X_NUMFCH AS ticket_number,
	g6x.G6X_VLRREI AS receipt,
	g6x.G6X_VLRDES AS expenses,
	g6x.G6X_VLRLIQ AS net_value
FROM G6X010 g6x
INNER JOIN GI6010 gi6
	ON gi6.GI6_CODIGO = g6x.G6X_AGENCI
WHERE g6x.G6X_DTREME = '{emission_date}'
	AND g6x.D_E_L_E_T_ = ''
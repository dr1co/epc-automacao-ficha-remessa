SELECT
	pv.empresa_id AS associated_company,
	tee.desctipoevento AS bill_description,
    tee.natureza AS nature,
	sum(COALESCE(cdp.importe,0)) AS bill_value
FROM
	caja_diversos cd
JOIN evento_extra ee
ON
	ee.eventoextra_id = cd.eventoextra_id
JOIN tipo_evento_extra tee
ON
	tee.tipoeventoextra_id = ee.tipoeventoextra_id
JOIN caja_diversos_pago cdp
	ON cdp.cajadiversos_id = cd.cajadiversos_id
 JOIN conta_corrente_ptovta ccp
	ON ccp.empresa_id = ee.empresa_id
	AND ccp.puntoventa_id = cd.puntoventa_id
	AND ccp.turno_id      = cd.turno_id
	AND ccp.usuario_id    = cd.usuario_id
	AND ccp.feccorte      = cd.feccorte
JOIN punto_venta pv
	ON pv.puntoventa_id = cd.puntoventa_id
WHERE (
	cd.indreimpresion = 0
	OR cd.indreimpresion IS NULL
)
	AND pv.nombpuntoventa = '{agency_name}'
	AND ccp.feccorte >= '{start_date}'
    AND ccp.feccorte < '{end_date}'
GROUP BY
	tee.desctipoevento,
    tee.natureza,
	pv.empresa_id
ORDER BY
	tee.desctipoevento
SELECT 
	pv.empresa_id AS associated_company,
	SUM(cj.preciopagado) AS ticket_total,
	SUM(cj.importetaxaembarque) AS boarding_tax_total,
	SUM(cj.importepedagio) AS toll_tax_total,
	SUM(cj.importeoutros) AS others_total,
	SUM(cj.importeseguro) AS insurance_total
FROM caja cj
INNER JOIN tipo_venta tv
	ON cj.tipoventa_id = tv.tipoventa_id
INNER JOIN punto_venta pv
	ON pv.puntoventa_id = cj.puntoventa_id
WHERE cj.activo = 1
    AND cj.fechorventa >= '{start_date}'
    AND cj.fechorventa < '{end_date}'
    AND pv.nombpuntoventa = '{agency_name}'
	AND cj.indstatusboleto = 'C'
GROUP BY
	pv.empresa_id
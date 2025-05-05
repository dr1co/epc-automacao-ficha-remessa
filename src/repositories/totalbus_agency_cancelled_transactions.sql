SELECT
	cj.numfoliosistema AS ticket_number,
	tv.desctipoventa AS selling_type,
	cj.preciopagado AS ticket_value,
	cj.importetaxaembarque AS boarding_tax,
	cj.importepedagio AS toll_tax,
	cj.indstatusboleto AS bill_status
FROM caja cj
INNER JOIN tipo_venta tv
	ON cj.tipoventa_id = tv.tipoventa_id
INNER JOIN punto_venta pv
	ON pv.puntoventa_id = cj.puntoventa_id
WHERE cj.activo = 1
    AND cj.fechorventa >= '{start_date}' 
	AND cj.fechorventa < '{end_date}'
	AND pv.nombpuntoventa = '{agency_name}'
	AND pv.empresa_id = '{associated_company}'
	AND cj.indstatusboleto = 'C'
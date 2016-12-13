# coding=utf-8
# -*- coding: utf-8 -*-
import urllib2
import datetime
import json
import operator
import psycopg2
import sys
reload(sys)  # UTF 8
sys.setdefaultencoding('UTF8')

import decimal
from decimal import *

import time



# FUNCTIONS

def flagSiNo(str):
	if str == "1":
		return "Si"
	elif str == "0":
		return "No"
	else:
		return "---"


def oneDec(num):
	num = Decimal(num).quantize(Decimal('1.00'))
	return num

unidadesEjecutoras = {
		1027: 'REGION LIMA',
		1325: 'REGION LIMA - SUB GERENCIA REGIONAL LIMA SUR',
		1228: 'REGION LIMA - DIRECCIÓN REGIONAL DE AGRICULTURA LIMA PROVINCIAS',
		1190: 'REGION LIMA - DIRECCIÓN REGIONAL DE EDUCACION LIMA	PROVINCIAS',
		1181: 'REGION LIMA - UNIDAD DE GESTIÓN EDUCATIVA LOCAL CAÑETE',
		1182: 'REGION LIMA - UNIDAD DE GESTIÓN EDUCATIVA LOCAL HUAURA',
		1183: 'REGION LIMA - UNIDAD DE GESTIÓN EDUCATIVA LOCAL HUARAL',
		1184: 'REGION LIMA - UNIDAD DE GESTIÓN EDUCATIVA LOCAL CAJATAMBO',
		1185: 'REGION LIMA - UNIDAD DE GESTIÓN EDUCATIVA LOCAL CANTA',
		1186: 'REGION LIMA - UNIDAD DE GESTIÓN EDUCATIVA LOCAL YAUYOS',
		1187: 'REGION LIMA - UNIDAD DE GESTIÓN EDUCATIVA LOCAL OYON',
		1188: 'REGION LIMA - UNIDAD DE GESTIÓN EDUCATIVA LOCAL HUAROCHIRI',
		1189: 'REGION LIMA - UNIDAD DE GESTIÓN EDUCATIVA LOCAL BARRANCA',
		1285: 'REGION LIMA - DIRECCION DE SALUD III	LIMA NORTE',
		1286: 'REGION LIMA - HOSP.HUACHO - HUAURA - OYON Y SERV.BASICOS DE SALUD',
		1287: 'REGION LIMA - SERVICIOS BASICOS DE SALUD	CAÑETE - YAUYOS',
		1288: 'REGION LIMA - HOSPITAL DE APOYO REZOLA',
		1289: 'REGION LIMA - HOSP.BARRANCA - CAJATAMBO Y SERV.BASICOS DE SALUD',
		1290: 'REGION LIMA - HOSP.CHANCAY Y SERVICIOS BASICOS DE SALUD',
		1291: 'REGION LIMA - SERV.BASICOS DE SALUD CHILCA - MALA',
		1292: 'REGION LIMA - HOSPITAL HUARAL Y SERVICIOS BASICOS DE	SALUD',
		1404: 'GOB.REG.DE LIMA - RED DE	SALUD DE HUAROCHIRÍ'}

# Connect to an existing database
conn = psycopg2.connect("dbname=sayhuite_gr user=postgres password=postgres")
# Open a cursor to perform database operations
cur = conn.cursor()

# Query the database and obtain data as Python objects
print ('Obteniendo Datos...')
cur.execute("SELECT cod_snip, cod_unif as id FROM grli_pip_total_priori order by id")
curi = cur.fetchall()
for i in curi:

	print ('********************************************************************************************* CODIGO SNIP <<' + i[0] +'>> **********************************************************************************************' )

	#VARS
	#codigoSnip = raw_input('Ingrese Codigo Snip: ')
	codigoSnip,codigoSiaf, flagSnip = i[0],i[1], 'true'

	print 'Consultando a SOSEM ...'

	#INFO SNIP
	v = dict.fromkeys([
							'_nom_proyec',
							'_cod_snip',
							'_cod_unif',
							'_u_formul',
							'_u_ejec',
							'_sector',
							'_m_pip',
							'_m_viab',
							'_m_exptec',
							'_est_proyec',
							'_m_pim',
							'_f_pim',
							'_m_pim_acu',
							'_f_pim_acu',
							'_m_deveng',
							'_m_deveng_a',
							'_f_deveng_a',
							'_f_adjudica',
							'_m_eject',
							'_nro_contrato',
							'_f_i_obra',
							'_f_f_obra',
							'_t_ejec_dia',
							'_a_fisico',
							'_a_financ',
							'_anio_proye',
							'_anio_pic',
							'_mpp_pic',
							'_macr_pic',
							'_mpia_pic',
							'_estadp_pic',
							'_progr',
							'_subprogr',
							'fuente'], "")

	########################################################################################################################
	#################################################### INFO SNIP #########################################################
	########################################################################################################################
	response = ''
	infoSnip = ''
	qVar = 'cod_snip'
	## dd/mm/yyyy format
	v['_upDate'] = time.strftime("%d-%m-%Y")
	if codigoSnip == 'SIN COD.':
		codigoSnip = i[1]
		flagSnip = 'false'
		qVar = 'cod_unif'

	try:
		response = urllib2.urlopen("http://ofi5.mef.gob.pe/sosem2/wsSosem.asmx/ListarInfoSnip?flagsnip="+ flagSnip +"&codigo="+ codigoSnip).read()

		ini = response.find('[')
		fin = response.find(']<')

		jsonstr = response[ini + 1:fin]

		#print jsonstr

		infoSnip = json.loads(jsonstr)

	except Exception:
		print "Error al consultar informacion"

	if len(infoSnip) > 0:
		response = ''
		infoObras = ''
		########################################################################################################################
		#################################################### INFO OBRAS #########################################################
		########################################################################################################################
		try:
			response = urllib2.urlopen("http://ofi5.mef.gob.pe/sosem2/wsSosem.asmx/ListarInfoObras?codigo=" + codigoSnip).read()
			ini = response.find('[')
			fin = response.find(']<')

			jsonstr = response[ini + 1:fin]

			#print jsonstr

			infoObras = json.loads(jsonstr)
		except Exception:
			print "No hay datos en InfoObras"

		print ('Nombre PIP -> ' + infoSnip['Nombre'])
		v['_nom_proyec'] = infoSnip['Nombre']
		print ('Unidad Formuladora (UF) -> ' + infoSnip['Uf'])
		v['_u_formul'] = infoSnip['Uf']
		if infoObras != '':
			#print ('Unidad Ejecutora -> ' + infoObras['Ejecutora'])
			print ('Inicio de Obra -> ' + infoObras['InicioObra'])
			v['_f_i_obra'] = infoObras['InicioObra']

		print ('Sector -> ' + infoSnip['Funcion'])
		v['_sector'] = infoSnip['Funcion']
		if infoSnip['MontoF16'] != 0:
			print ('Monto PIP -> ' +  "{:,}".format(infoSnip['MontoF16']))
			v['_m_pip'] = "{:,}".format(infoSnip['MontoF16'])
		else:
			print ('Monto PIP -> ' + "{:,}".format(infoSnip['Costo']))
			v['_m_pip'] = "{:,}".format(infoSnip['Costo'])
		print ('Monto Viable -> ' + "{:,}".format(infoSnip['MontoAlternativa']))
		v['_m_viab'] = "{:,}".format(infoSnip['MontoAlternativa'])
		print ('Monto Expediente Tecnico -> ' + "{:,}".format(infoSnip['MontoF15']))
		v['_m_exptec'] = "{:,}".format(infoSnip['MontoF15'])
		print ('Estado del proyecto -> ' + str(infoSnip['Situacion']))
		v['_est_proyec'] = str(infoSnip['Situacion'])
		print ('Programa -> ' + str(infoSnip['Programa']))
		v['_progr'] = str(infoSnip['Programa'])
		print ('Sub-Programa -> ' + str(infoSnip['Subprograma']))
		v['_subprogr'] = str(infoSnip['Subprograma'])
	else:
		print('No se encontraron datos con ese codigo snip')

	########################################################################################################################
	#################################################### INFO SIAF #########################################################
	########################################################################################################################

	response = ''
	data = ''
	try:
		response = urllib2.urlopen(
			"http://ofi5.mef.gob.pe/sosem2/wsSosem.asmx/ListarInfoSiaf?flagsnip=" + flagSnip + "&codigo=" + codigoSnip).read()
		ini = response.find('[')
		fin = response.find(']<')

		jsonstr = response[ini + 1:fin]
		#print jsonstr
		data = json.loads(jsonstr)
	except Exception:
		print ("Error al consultar informacion InfoSiaf")



	if len(data) > 0:

		html = ""
		seac = ""
		# for i in data:
		#	row = data[i]
		id = data['CodigoSiaf']
		#	print row

		uELIST = unidadesEjecutoras.keys()

		print ('Código SIAF -> ' + id)
		print ('Nombre del Proyecto -> ' + data['Nombre'])
		v['_nom_proyec'] = data['Nombre']
		print ('Tipo de Proyecto -> ' + data['TipoProyectoSnip'])
		#print ('PIM Acumulado -> ' + str(data['Pim']))
		#print ('Devengado Acumulado -> ' + str(data['Devengado']))

		# print '-------------------------------------- EJECUTORAS ------------------------------------'
        #
		# if len(data['Ejecutoras']) > 0:
		# 	#print data['Ejecutoras']
		# 	cantidadEjecutoras = len(data['Ejecutoras'])
		# 	lastYear = ''
		# 	PimRegionalPasado = Decimal(0.00)
		# 	DevRegionalPasado = Decimal(0.0)
		# 	PimRegionalActual = Decimal(0.0)
		# 	DevRegionalActual = Decimal(0.0)
		# 	i = 0
        #
		# 	for e in reversed(data['Ejecutoras']):
		# 		if int(e['Codigo']) in uELIST and lastYear == '':
		# 			lastYear = e[b'Año'.decode('UTF-8')]
		# 			PimRegionalActual = PimRegionalActual + oneDec(e['MontoPim'])
		# 			DevRegionalActual = DevRegionalActual + oneDec(e['MontoDev'])
		# 		elif int(e['Codigo']) in uELIST:
        #
		# 			if e[b'Año'.decode('UTF-8')] == lastYear:
		# 				PimRegionalActual = PimRegionalActual + oneDec(e['MontoPim'])
		# 				DevRegionalActual = DevRegionalActual + oneDec(e['MontoDev'])
		# 			else:
		# 				PimRegionalPasado = PimRegionalPasado + oneDec(e['MontoPim'])
		# 				DevRegionalPasado = DevRegionalPasado + oneDec(e['MontoDev'])
		# 		else:
		# 			print 'Año Pasado -> ' + e[b'Año'.decode('UTF-8')]
        #
		# 	print 'Por ejecutora ------------------'
		# 	print 'Año mas reciente de ejecucion -> ' + str(lastYear)
		# 	print 'Pim Regional Pasado -> ' + str(PimRegionalPasado)
		# 	print 'Devengado Regional Pasado -> ' + str(DevRegionalPasado)
		# 	print 'Pim Regional Actual -> ' + str(PimRegionalActual)
		# 	print 'Devengado Regional Actual -> ' + str(DevRegionalActual)
		# 	print '-----------------------------------------------------------------------------------------------'

		#else:
		#	print 'No hay ejecutoras'

		PimRegionalPasado = Decimal(0.00)
		DevRegionalPasado = Decimal(0.00)
		PimRegionalActual = Decimal(0.00)
		DevRegionalActual = Decimal(0.00)
		PIA = Decimal(0.00)
		#----------------------Fuente Financiera--------------------
		print('---------------------------------INFORMACION FINANCIERA-------------------------------')
		if len(data['InfoFinanciera']) > 0:

			#OBTENGO AÑO MAS RECIENTE
			lastYear = 0
			for e in (data['InfoFinanciera']):
				if int(e['CodigoEjecutora']) in uELIST and e[b'Año'.decode('UTF-8')] > lastYear :
					lastYear = e[b'Año'.decode('UTF-8')]
			print 'Primer valor last Year -> ' + str(lastYear)
			ejecutoras = {}
			for e in reversed(data['InfoFinanciera']):
				if int(e['CodigoEjecutora']) in uELIST:
					if e[b'Año'.decode('UTF-8')] == lastYear:
						PimRegionalActual = PimRegionalActual + oneDec(e['Pim'])
						DevRegionalActual = DevRegionalActual + oneDec(e['Dev'])
						PIA = v['_mpia_pic']

						if int(e['CodigoEjecutora']) in ejecutoras.keys():
							ejecutoras[int(e['CodigoEjecutora'])] += oneDec(e['Pim'])
						else:
							ejecutoras[int(e['CodigoEjecutora'])] = oneDec(e['Pim'])
					elif PimRegionalActual == 0.00 or PimRegionalActual == 0.0 or PimRegionalActual == 0 and e[b'Año'.decode('UTF-8')] < lastYear:
						 PimRegionalActual = PimRegionalActual + oneDec(e['Pim'])
						 DevRegionalActual = DevRegionalActual + oneDec(e['Dev'])
						 v['_upDate'] =  '31-12-' + str(e[b'Año'.decode('UTF-8')])
						 print 'no'

				PimRegionalPasado = PimRegionalPasado + oneDec(e['Pim'])
				DevRegionalPasado = DevRegionalPasado + oneDec(e['Dev'])
				#else:
					#print 'Año Pasado -> ' + e[b'Año'.decode('UTF-8')]
			#print ejecutoras
			print 'Por financiera ------------------'
			print 'Unidad ejecutora -> ' + unidadesEjecutoras[max(ejecutoras.iteritems(), key=operator.itemgetter(1))[0]]
			v['_u_ejec'] = unidadesEjecutoras[max(ejecutoras.iteritems(), key=operator.itemgetter(1))[0]]
			print 'Año mas reciente de ejecucion -> ' + str(lastYear)
			print 'Pim Regional Pasado -> ' + "{:,}".format(PimRegionalPasado)
			v['_m_pim_acu'] = "{:,}".format(PimRegionalPasado)
			print 'Devengado Regional Pasado -> ' + "{:,}".format(DevRegionalPasado)
			v['_m_deveng_a'] = "{:,}".format(DevRegionalPasado)
			print 'Pim Regional Actual -> ' + "{:,}".format(PimRegionalActual)
			v['_m_pim'] = "{:,}".format(PimRegionalActual)
			print 'Devengado Regional Actual -> ' + "{:,}".format(DevRegionalActual)
			v['_m_deveng'] = "{:,}".format(DevRegionalActual)

			if DevRegionalActual != 0 or DevRegionalActual != 0.0 or DevRegionalActual != 0.00:
				print 'AVANCE FINANCIERO ->' + str(oneDec(Decimal(str(DevRegionalActual)) / Decimal(str(PimRegionalActual)) * 100))
				v['_a_financ'] = str(oneDec(Decimal(str(DevRegionalActual)) / Decimal(str(PimRegionalActual)) * 100))
			else:
				print 'AVANCE FINANCIERO ->' + '0'
				v['_a_financ'] = '0'
		else:
			print 'No hay InfoFinanciera'

		# ----------------------Montos Siaf--------------------

		if len(data['MontosSiaf']) > 0:
			cantidadMontosSiaf = len(data['MontosSiaf'])
			i = 0
			for e in data['MontosSiaf']:
				i = i + 1
				#print str(e)
				if (i == cantidadMontosSiaf):
					item = e
					print ('Pia: ' + "{:,}".format(item['Pia']))
				else:
					continue
		else:
			print 'No hay InfoSiaf'

	else:
		print('No se encontraron datos financieros con ese codigo snip')





	########################################################################################
	####################################### ACTUALIZAR CAMPOS ##############################

	updateRecords = "UPDATE grli_pip_total_priori " \
					"SET " \
					"nom_proyec = (CASE WHEN '" + v['_nom_proyec'] + "' = nom_proyec THEN nom_proyec WHEN '" + v['_nom_proyec'] + "' = '' THEN nom_proyec ELSE '" + v['_nom_proyec'] + "' END)," \
					"u_formul   = (CASE WHEN '" + v['_u_formul'] + "' = u_formul THEN u_formul WHEN '" + v['_u_formul'] + "' = '' THEN u_formul ELSE '" + v['_u_formul'] + "' END)," \
					"u_ejec     = (CASE WHEN '" + v['_u_ejec'] + "' = u_ejec THEN u_formul WHEN '" + v['_u_ejec'] + "' = '' THEN u_ejec ELSE '" + v['_u_ejec'] + "' END)," \
					"sector     = (CASE WHEN '" + v['_sector'] + "' = sector THEN sector WHEN '" + v['_sector'] + "' = '' THEN sector ELSE '" + v['_sector'] + "' END)," \
					"m_pip      = (CASE WHEN '" + v['_m_pip'] + "' = m_pip THEN m_pip WHEN '" + v['_m_pip'] + "' = '0.0' THEN m_pip WHEN '" + v['_m_pip'] + "' = '0.00' THEN m_pip WHEN '" + v['_m_pip'] + "' = '0' THEN m_pip WHEN '" + v['_m_pip'] + "' = '' THEN m_pip ELSE '" + v['_m_pip'] + "' END)," \
					"m_viab     = (CASE WHEN '" + v['_m_viab'] + "' = m_viab THEN m_viab WHEN '" + v['_m_viab'] + "' = '0.0' THEN m_viab WHEN '" + v['_m_viab'] + "' = '0.00' THEN m_pip WHEN '" + v['_m_viab'] + "' = '0' THEN m_viab WHEN '" + v['_m_viab'] + "' = '' THEN m_viab ELSE '" + v['_m_viab'] + "' END)," \
					"m_exptec   = (CASE WHEN '" + v['_m_exptec'] + "' = m_exptec THEN m_exptec WHEN '" + v['_m_exptec'] + "' = '0.0' THEN m_exptec WHEN '" + v['_m_exptec'] + "' = '0.00' THEN m_exptec WHEN '" + v['_m_exptec'] + "' = '0' THEN m_exptec WHEN '" + v['_m_exptec'] + "' = '' THEN m_exptec ELSE '" + v['_m_exptec'] + "' END)," \
					"est_proy   = (CASE WHEN '" + v['_est_proyec'] + "' = est_proy THEN est_proy WHEN '" + v['_est_proyec'] + "' = '' THEN est_proy ELSE '" + v['_est_proyec'] + "' END)," \
					"m_pim      = (CASE WHEN '" + v['_m_pim'] + "' = m_pim THEN m_pim WHEN '" + v['_m_pim'] + "' = '0.0' THEN m_pim WHEN '" + v['_m_pim'] + "' = '0.00' THEN m_pim WHEN '" + v['_m_pim'] + "' = '0' THEN m_pim WHEN '" + v['_m_pim'] + "' = '' THEN m_pim ELSE '" + v['_m_pim'] + "' END)," \
					"f_pim      = (CASE WHEN '" + v['_upDate'] + "' = f_pim THEN f_pim WHEN '" + v['_upDate'] + "' = '' THEN f_pim ELSE '" + v['_upDate'] + "' END)," \
					"m_pim_acu  = (CASE WHEN '" + v['_m_pim_acu'] + "' = m_pim_acu THEN m_pim_acu WHEN '" + v['_m_pim_acu'] + "' = '0.0' THEN m_pim_acu WHEN '" + v['_m_pim_acu'] + "' = '0.00' THEN m_pim_acu WHEN '" + v['_m_pim_acu'] + "' = '0' THEN m_pim_acu WHEN '" + v['_m_pim_acu'] + "' = '' THEN m_pim_acu ELSE '" + v['_m_pim_acu'] + "' END)," \
					"f_pim_acu  = (CASE WHEN '" + v['_upDate'] + "' = f_pim_acu THEN f_pim_acu WHEN '" + v['_upDate'] + "' = '' THEN f_pim_acu ELSE '" + v['_upDate'] + "' END)," \
					"m_deveng   = (CASE WHEN '" + v['_m_deveng'] + "' = m_deveng THEN m_deveng WHEN '" + v['_m_deveng'] + "' = '0.0' THEN m_deveng WHEN '" + v['_m_deveng'] + "' = '0.00' THEN m_deveng WHEN '" + v['_m_deveng'] + "' = '0' THEN m_deveng WHEN '" + v['_m_deveng'] + "' = '' THEN m_deveng ELSE '" + v['_m_deveng'] + "' END)," \
					"f_deveng   = (CASE WHEN '" + v['_upDate'] + "' = f_deveng THEN f_deveng_a WHEN '" + v['_upDate'] + "' = '' THEN f_deveng ELSE '" + v['_upDate'] + "' END)," \
					"m_deveng_a = (CASE WHEN '" + v['_m_deveng_a'] + "' = m_pip THEN m_deveng_a WHEN '" + v['_m_deveng_a'] + "' = '0.0' THEN m_deveng_a WHEN '" + v['_m_deveng_a'] + "' = '0.00' THEN m_deveng_a WHEN '" + v['_m_deveng_a'] + "' = '0' THEN m_deveng_a WHEN '" + v['_m_deveng_a'] + "' = '' THEN m_deveng_a ELSE '" + v['_m_deveng_a'] + "' END)," \
					"a_financ   = (CASE WHEN '" + v['_a_financ'] + "' = a_financ THEN a_financ WHEN '" + v['_a_financ'] + "' = '' THEN a_financ ELSE '" + v['_a_financ'] + "' END)," \
					"mpia_pic   = (CASE WHEN '" + v['_mpia_pic'] + "' = m_pip THEN mpia_pic WHEN '" + v['_mpia_pic'] + "' = '0.0' THEN mpia_pic WHEN '" + v['_mpia_pic'] + "' = '0.00' THEN mpia_pic WHEN '" + v['_mpia_pic'] + "' = '0' THEN mpia_pic WHEN '" + v['_mpia_pic'] + "' = '' THEN mpia_pic ELSE '" + v['_mpia_pic'] + "' END)," \
					"f_deveng_a = (CASE WHEN '" + v['_upDate'] + "' = f_deveng_a THEN f_deveng_a WHEN '" + v['_upDate'] + "' = '' THEN f_deveng_a ELSE '" + v['_upDate'] + "' END)," \
					"f_adjudica = (CASE WHEN '" + v['_f_adjudica'] + "' = f_adjudica THEN f_adjudica WHEN '" + v['_f_adjudica'] + "' = '' THEN f_adjudica ELSE '" + v['_f_adjudica'] + "' END)," \
					"f_i_obra   = (CASE WHEN '" + v['_f_i_obra'] + "' = f_i_obra THEN f_i_obra WHEN '" + v['_f_i_obra'] + "' = '' THEN f_i_obra ELSE '" + v['_f_i_obra'] + "' END)," \
					"progr      = (CASE WHEN '" + v['_progr'] + "' = progr THEN progr WHEN '" + v['_progr'] + "' = '' THEN progr ELSE '" + v['_progr'] + "' END),"\
					"subprogr   = (CASE WHEN '" + v['_subprogr'] + "' = subprogr THEN subprogr WHEN '" + v['_subprogr'] + "' = '' THEN subprogr ELSE '" + v['_subprogr'] + "' END),"\
					"f_f_obra   = (CASE WHEN '" + v['_f_f_obra'] + "' = f_f_obra THEN f_f_obra WHEN '" + v['_f_f_obra'] + "' = '' THEN f_f_obra ELSE '" + v['_f_f_obra'] + "' END)"  \
					"WHERE " \
					""+ qVar +"   = '" + codigoSnip + "';"

        cur.execute(updateRecords)
        conn.commit()

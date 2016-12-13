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


# FUNCTIONS

def flagSiNo(str):
	if str == "1":
		return "Si"
	elif str == "0":
		return "No"
	else:
		return "---"


def oneDec(num):
	num = Decimal(num).quantize(Decimal('1.0'))
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

thisYear = datetime.datetime.now().year

i = []
i[0] = 2106392
codigoSnip,codigoSiaf, flagSnip = i[0],i[1], 'true'

if 1:

	print ('********************************************************************************************* CODIGO SNIP <<' + i[0] +'>> **********************************************************************************************' )

	#VARS
	#codigoSnip = raw_input('Ingrese Codigo Snip: ')


	print 'Consultando a SOSEM ...'

	#INFO SNIP
	v = dict.fromkeys([
							'_nom_proy',
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

        if codigoSnip == 'SIN COD.':
            codigoSnip = i[1]
            flagSnip = 'false'
	########################################################################################################################
	#################################################### INFO SNIP #########################################################
	########################################################################################################################
	response = ''
	infoSnip = ''
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
		v['_nom_proy'] = infoSnip['Nombre']
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
		print ('Sub-Programa -> ' + str(infoSnip['SubPrograma']))
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
		print ('Tipo de Proyecto -> ' + data['TipoProyectoSnip'])
		#print ('PIM Acumulado -> ' + str(data['Pim']))
		#print ('Devengado Acumulado -> ' + str(data['Devengado']))

		# print '-------------------------------------- EJECUTORAS ------------------------------------'
        #
		# if len(data['Ejecutoras']) > 0:
		# 	#print data['Ejecutoras']
		# 	cantidadEjecutoras = len(data['Ejecutoras'])
		# 	lastYear = ''
		# 	PimRegionalPasado = Decimal(0.0)
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

		PimRegionalPasado = Decimal(0.0)
		DevRegionalPasado = Decimal(0.0)
		PimRegionalActual = Decimal(0.0)
		DevRegionalActual = Decimal(0.0)
		#----------------------Fuente Financiera--------------------
		print('---------------------------------INFORMACION FINANCIERA-------------------------------')
		if len(data['InfoFinanciera']) > 0:

			#OBTENGO AÑO MAS RECIENTE
			lastYear = 0
			for e in (data['InfoFinanciera']):
				if int(e['CodigoEjecutora']) in uELIST and e[b'Año'.decode('UTF-8')] > lastYear :
					lastYear = e[b'Año'.decode('UTF-8')]

			ejecutoras = {}
			for e in data['InfoFinanciera']:
				if int(e['CodigoEjecutora']) in uELIST:
					if e[b'Año'.decode('UTF-8')] == lastYear:
						PimRegionalActual = PimRegionalActual + oneDec(e['Pim'])
						DevRegionalActual = DevRegionalActual + oneDec(e['Dev'])

						if int(e['CodigoEjecutora']) in ejecutoras.keys():
							ejecutoras[int(e['CodigoEjecutora'])] += oneDec(e['Pim'])
						else:
							ejecutoras[int(e['CodigoEjecutora'])] = oneDec(e['Pim'])
					else:
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
			print 'AVANCE FINANCIERO ->' + str(v['_m_deveng'] / v['_m_pim'])
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





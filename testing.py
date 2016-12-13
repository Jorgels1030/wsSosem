# coding=utf-8
# -*- coding: utf-8 -*-
import urllib2
import json
import psycopg2
import sys
reload(sys)  # Reload does the trick!
sys.setdefaultencoding('UTF8')

#FUNCTIONS


#VARS
codSnip='96603'

########################################################################################################################
#################################################### INFO SIAF #########################################################
########################################################################################################################

response = ''

try:
	response = urllib2.urlopen("http://ofi5.mef.gob.pe/sosem2/wsSosem.asmx/ListarInfoSiaf?flagsnip=true&codigo=" + codSnip).read()
except Exception:
    print ("Error al consultar informacion")

ini = response.find('[')
fin = response.find(']<')

jsonstr = response[ini + 1:fin]
print jsonstr
data = json.loads(jsonstr)

if len(data) > 0:

    html = ""
    seac = ""
    # for i in data:
    #	row = data[i]
    id = data['CodigoSiaf']
    #	print row


    # id = id.substr(0, 1) + "." + id.substr(1);
    print ('Código SIAF -> ' + id)
    print ('Nombre del Proyecto -> ' + data['Nombre'])
    print ('Tipo de Proyecto -> ' + data['TipoProyectoSnip'])
    print ('PIM Acumulado -> ' + str(data['Pim']))
    print ('Devengado Acumulado -> ' + str(data['Devengado']))
    print ('Año-Mes del primer devengado -> ' + data['InfoPeriodoPrimerDevengado'])
    print ('Año-Mes del último devengado: -> ' + data['InfoPeriodoUltimoDevengado'])
    # if len(data['MontosSiaf']) > 0:
    # print 'MontoSiaf -> ' + str(i)

    # m_pim
    # f_pim
    # m_pim_acu
    # m_deveng
    # f_deveng
    # m_deveng_a
    # f_deveng_a
    # f_adjudica
    # m_ejec
    # nro_contrato
    # f_i_obra
    # f_f_obra
    # t_ejec_dia
    # a_fisico
    # a_financ
    # f_tdr
    # a_tdr
    # f_perfil
    # a_perfil
    # f_expec
    # a_expec
    # f_ejec
    # mpia_pic
    # a_ejec


    # ---------------- EJECUTORAS --------------

    print '-------------------------------------- EJECUTORAS ------------------------------------'

    if len(data['Ejecutoras']) > 0:
        cantidadEjecutoras = len(data['Ejecutoras'])
        i = 0
        for e in data['Ejecutoras']:
            i = i + 1
            if(i == cantidadEjecutoras):
                anioActual = e[(b'Año').decode('utf-8')]
                item = e
                print ('Nombre: ' + item['Nombre'])
                if item['CodigoTipoEjecutora'] == "E":
                    print 'Sector Nombre' + item.SectorNombre + " - " + item['PliegoNombre']
                if item['CodigoTipoEjecutora'] == "R":
                    print 'Departament :' + item['DepartamentoNombre']
                if item['CodigoTipoEjecutora'] == "M":
                    print item['DepartamentoNombre'] + " - " + item['ProvinciaNombre']

                print 'Monto PIM -> ' + str(item['MontoPim'])
                print 'Monto Dev -> ' + str(item['MontoDev'])
                print 'Pim Actual -> ' + str(item['PimActual'])
                print 'Pim Acumulado -> ' + str(item['PimAcumulado'])
                print 'Dev Acumulado -> ' + str(item['DevAcumulado'])
                print ('---------------------------------------------')
            else:
                continue

    else:
        print 'No hay ejecutoras'

else:
	print('No se encontraron datos con ese codigo snip')































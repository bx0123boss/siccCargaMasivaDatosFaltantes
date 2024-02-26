/*
 **********************************************************************************************************
 *    FECHA         ID.REQUERIMIENTO            DESCRIPCIÓN                            MODIFICÓ.          
 * 09/02/2015             3226            Creación del documento con            José Manuel Zúñiga Flores 
 *                                        especificaciones de la Carga de       Fernando Vázquez Galán    
 *                                        Archivo para Datos Faltantes de                                 
 *                                        Calificación de Cartera                                         
 *                                                                                                        
 **********************************************************************************************************
 */
package OrionSFI.core.siccCargaMasivaDatosFaltantes;

import OrionSFI.core.commons.JDBCConnectionPool;
import OrionSFI.core.commons.MensajesSistema;
import OrionSFI.core.commons.SQLProperties;
import OrionSFI.core.institucion.Institucion;
import OrionSFI.core.reportesDEPP.ReportesDEPP;
import OrionSFI.reportes.Reporte;
import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.commons.io.FileUtils;

public class ReporteCargaMasivaSicc {

    Institucion institucion = new Institucion();
    List<String> listData = new ArrayList<>();
    short numeroInstitucion;
    int numeroLayout;
    String claveSistema, idUsuario, serialNumber, pathReporte, claveReporte, fechaReporte, folioLote, nombreLayout;
    StringBuffer query = null;
    JDBCConnectionPool bd = null;
    ResultSet resultado = null;
    ReportesDEPP reportesDEPP = new ReportesDEPP();
    SQLProperties sqlProperties = new SQLProperties();

    public ReporteCargaMasivaSicc() {
    }

    public ReporteCargaMasivaSicc(String serialNumber, String idUsuario,
            String foliolote, String pathReporte, String claveReporte,
            int numeroLayout) {
        this.serialNumber = serialNumber;
        this.idUsuario = idUsuario;
        this.folioLote = foliolote;
        this.pathReporte = pathReporte;
        this.claveReporte = claveReporte;
        this.numeroLayout = numeroLayout;
    }

    public void getNombreLayout() throws Exception {
        MensajesSistema msjSistema = new MensajesSistema();
        try {
            bd = new JDBCConnectionPool();

            query = new StringBuffer("");
            query.append("SELECT NOMBRE_LAYOUT ");
            query.append("FROM SICC_CATALOGO_LAYOUT ");
            query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
            query.append("  AND NUMERO_LAYOUT = ");
            query.append(numeroLayout);
            System.out.println(query);
            resultado = bd.executeQuery(query.toString());
            if (resultado.next()) {
                nombreLayout = resultado.getString("NOMBRE_LAYOUT");
            }

            bd.close();
        } catch (SQLException e) {
            bd.close();
            e.printStackTrace();
            throw new Exception(msjSistema.getMensaje(131));
        }
    }

    public void getTablaReporte() throws Exception {
        numeroInstitucion = institucion.getNumeroInstitucion(serialNumber);
        creaTabla();
    }

    private void creaTabla() throws Exception {
        Date date = new Date();
        SQLProperties sqlproperties = new SQLProperties();
        SimpleDateFormat formatoFecha = new SimpleDateFormat(sqlproperties.getFormatoSoloFecha());
        fechaReporte = sqlproperties.getFechaInsercionFormateada(formatoFecha.format(date));

        try {
            bd = new JDBCConnectionPool();
            bd.setAutoCommit(false);

            query = new StringBuffer();
            query.append("INSERT INTO REP_435_DETALLE ");
            query.append("SELECT ").append(numeroInstitucion);
            query.append("  ,( ");
            query.append("    ROW_NUMBER() OVER ( ");
            query.append("      ORDER BY STL.NUMERO_CONSECUTIVO ");
            query.append("      ) ");
            query.append("    ) ");
            query.append("  ,").append(numeroLayout);
            query.append("  ,SCL.NOMBRE_LAYOUT ");
            query.append("  ,STL.FOLIO_LOTE ");
            query.append("  ,STL.NOMBRE_ARCHIVO ");
            query.append("  ,'").append(claveReporte).append("' ");
            query.append("  ,'").append(fechaReporte).append("' ");
            query.append("  ,STL.NUMERO_CONSECUTIVO ");
            if (numeroLayout != 2 && numeroLayout != 4 && numeroLayout != 3) {
            	if(numeroLayout == 1 || numeroLayout == 5 || numeroLayout == 6 || numeroLayout == 18|| numeroLayout == 19) {
                query.append("  ,STL.NUMERO_CLIENTE ");
                query.append("  ,( ");
                query.append("    CASE  ");
                query.append("      WHEN (VPDC.IND_TIPO_PERSONA = 3) ");
                query.append("        THEN (ISNULL(VPDC.RAZON_SOCIAL + ' ', '') + ISNULL(VPDC.DESCRIPCION_TIPO_SOCIEDAD, '')) ");
                query.append("      ELSE (ISNULL(VPDC.NOMBRE1 + ' ', '') + ISNULL(VPDC.NOMBRE2 + ' ', '') + ISNULL(VPDC.NOMBRES_ADICIONALES + ' ', '') + ISNULL(VPDC.APELLIDO_PATERNO + ' ', '') + ISNULL(VPDC.APELLIDO_MATERNO, '')) ");
                query.append("      END ");
                query.append("    ) AS NOMBRE_CLIENTE ");
                query.append("  ,NULL ");
                query.append("  ,NULL ");
            	}
            	if (numeroLayout == 7) {
                    query.append("  ,NULL ");
                    query.append("  ,STL.NUM_ID ");
                    query.append("  ,NULL ");
                    query.append("  ,NULL ");
                }
            	if (numeroLayout == 8) {
                    query.append("  ,NULL ");
                    query.append("  ,STL.CUENTA_INTERNA ");
                    query.append("  ,NULL ");
                    query.append("  ,NULL ");
                }
            	if (numeroLayout == 9) {
                    query.append("  ,NULL");
                    query.append("  ,STL.CLAVE_ENTIDAD ");
                    query.append("  ,NULL ");
                    query.append("  ,NULL ");
                }
            	if (numeroLayout == 10) {
                    query.append("  ,NULL ");
                    query.append("  ,STL.CUENTA_CNBV  ");
                    query.append("  ,NULL ");
                    query.append("  ,NULL ");
                }
            	if (numeroLayout == 11) {
                    query.append("  ,NULL ");
                    query.append("  ,STL.CUENTA_CNBV_UPA ");
                    query.append("  ,NULL ");
                    query.append("  ,NULL ");
                }
            	if (numeroLayout == 12 ) {
                    query.append("  ,NULL ");
                    query.append("  ,STL.NUMERO_SUBSIDIARIA ");
                    query.append("  ,NULL ");
                    query.append("  ,NULL ");
                }
            	if (numeroLayout == 13) {
                    query.append("  ,NULL ");
                    query.append("  ,STL.TIPO_SUBSIDIARIA ");
                    query.append("  ,NULL ");
                    query.append("  ,NULL ");
                }
            	if (numeroLayout == 14 || numeroLayout == 15 || numeroLayout == 16 || numeroLayout == 17) {
                    query.append("  ,NULL ");
                    query.append("  ,STL.CUENTA_CNBV_R13 ");
                    query.append("  ,NULL ");
                    query.append("  ,NULL ");
                }
            } else {
                if(numeroLayout == 2 /*|| numeroLayout == 14*/) {
                    query.append("  ,VSC.NUMERO_CLIENTE ");
                } else {
                    query.append("  ,CRC.NUMERO_CLIENTE ");
                }
                query.append("  ,( ");
                query.append("    CASE  ");
                query.append("      WHEN (VPDC.IND_TIPO_PERSONA = 3) ");
                query.append("        THEN (ISNULL(VPDC.RAZON_SOCIAL + ' ', '') + ISNULL(VPDC.DESCRIPCION_TIPO_SOCIEDAD, '')) ");
                query.append("      ELSE (ISNULL(VPDC.NOMBRE1 + ' ', '') + ISNULL(VPDC.NOMBRE2 + ' ', '') + ISNULL(VPDC.NOMBRES_ADICIONALES + ' ', '') + ISNULL(VPDC.APELLIDO_PATERNO + ' ', '') + ISNULL(VPDC.APELLIDO_MATERNO, '')) ");
                query.append("      END ");
                query.append("    ) AS NOMBRE_CLIENTE ");
                if (numeroLayout == 2 /*|| numeroLayout == 14*/) {
                    query.append("  ,STL.NUMERO_CUENTA ");
                    query.append("  ,NULL ");
                }
                if (numeroLayout == 4 /*|| numeroLayout == 13*/) {
                    query.append("  ,NULL ");
                    query.append("  ,STL.NUMERO_LINEA ");
                }
                if (numeroLayout == 3) {
                    query.append("  ,NULL ");
                    query.append("  ,STL.NUMERO_GARANTIA ");
                }
            }
            query.append("  ,STL.STATUS_CARGA ");
            query.append("  ,STL.STATUS_ALTA ");
            query.append("  ,SCE.CODIGO_MENSAJE ");
            query.append("  ,SCE.DESCRIPCION_ERROR ");
            query.append("  ,'").append(idUsuario).append("' ");
            if (numeroLayout != 2 && numeroLayout != 4 && numeroLayout != 3 ) {
            	if(numeroLayout == 1 || numeroLayout == 5 || numeroLayout == 6 || numeroLayout == 18 || numeroLayout == 19) {
	                if (numeroLayout == 1) {
	                    query.append("FROM SICC_PERSONA_LO AS STL ");
	                }
	                if (numeroLayout == 5) {
	                    query.append("FROM SICC_BIENES_ADJUDICADOS_LO AS STL ");
	                }
	                if (numeroLayout == 6) {
	                    query.append("FROM SICC_OPERATIVO_PRESTAMO_LO AS STL ");
	                }
	                if (numeroLayout == 18) {
	                    query.append("FROM SICC_ESTADO_CUENTA_LO AS STL ");
	                }
	                if (numeroLayout == 19) {
	                    query.append("FROM SICC_ANEXO20_LO AS STL ");
	                }
	                query.append("LEFT JOIN V_PCLI_DATOS_CLIENTE AS VPDC ON STL.NUMERO_INSTITUCION = VPDC.NUMERO_INSTITUCION ");
	                query.append("  AND STL.NUMERO_CLIENTE = VPDC.NUMERO_CLIENTE ");
            	}
                if (numeroLayout == 7) {
                    query.append("FROM SICC_OPERATIVO_INTERBANCARIO_LO AS STL ");
                }
                if (numeroLayout == 8) {
                    query.append("FROM SICC_CATALOGO_MINIMO_LO AS STL ");
                }
                if (numeroLayout == 9) {
                    query.append("FROM SICC_OPERATIVOS_INVERSIONES_LO AS STL ");
                }
                if (numeroLayout == 10) {
                    query.append("FROM SICC_CONTABLES_RECLASIFICACIONES_LO AS STL ");
                }
                if (numeroLayout == 11) {
                    query.append("FROM SICC_CONTABLES_UPA_LO AS STL ");
                }
                if (numeroLayout == 12) {
                    query.append("FROM SICC_CONTABLES_CONSOLIDACION_LO AS STL ");
                }
                if (numeroLayout == 13) {
                    query.append("FROM SICC_CONTABLES_ELIMINACIONES_LO AS STL ");
                }
                if (numeroLayout == 14) {
                    query.append("FROM SICC_ESTADO_VARIACIONES_LO AS STL ");
                }
                if (numeroLayout == 15) {
                    query.append("FROM SICC_ESTADO_FLUJO_EFECTIVO_LO AS STL ");
                }
                if (numeroLayout == 16) {
                    query.append("FROM SICC_BALANCE_GENERAL_LO AS STL ");
                }
                if (numeroLayout == 17) {
                    query.append("FROM SICC_ESTADO_RESULTADOS_LO AS STL ");
                }
               
                //if (numeroLayout == 6) {
                //    query.append("FROM SICC_ANEXO21_LO AS STL ");
                //}
                //if (numeroLayout == 7) {
                //    query.append("FROM SICC_ANEXO22_LO AS STL ");
                //}
                //if (numeroLayout == 9) {
                //    query.append("FROM SICC_AVAL_LO AS STL ");
                //}
                
            } else {
                if (numeroLayout == 2 /*|| numeroLayout == 14*/) {
                	if (numeroLayout == 2){
                		query.append("FROM SICC_CREDITO_LO AS STL ");
                	} /*else if (numeroLayout == 14){
                		query.append("FROM SICC_ANEXO19_LO AS STL ");
                	}*/
                    query.append("LEFT JOIN V_SISTEMAS_CUENTAS VSC ON VSC.NUMERO_INSTITUCION = STL.NUMERO_INSTITUCION ");
                    query.append("  AND VSC.NUMERO_CUENTA = STL.NUMERO_CUENTA ");
                    query.append("  AND VSC.STATUS = 1 ");
                    query.append("  AND VSC.CLAVE_SISTEMA = 'CREDITO' ");
                } else {
                    if (numeroLayout == 4) {
                        query.append("FROM SICC_LINEA_CREDITO_LO AS STL ");
                        query.append("LEFT JOIN CTES_REL_CLIENTE_LINEA AS CRC ON STL.NUMERO_INSTITUCION = CRC.NUMERO_INSTITUCION ");
                        query.append("  AND STL.NUMERO_LINEA = CRC.NUMERO_LINEA ");
                    }
                    /*if (numeroLayout == 13) {
                        query.append("FROM SICC_CODIGO_CNBV_LO AS STL ");
                        query.append("LEFT JOIN CTES_REL_CLIENTE_LINEA AS CRC ON STL.NUMERO_INSTITUCION = CRC.NUMERO_INSTITUCION ");
                        query.append("  AND STL.NUMERO_LINEA = CRC.NUMERO_LINEA ");
                    }*/
                    if (numeroLayout == 3) {
                        query.append("FROM SICC_GARANTIA_LO AS STL ");
                        query.append("LEFT JOIN CTES_REL_CLIENTE_GARANTIAS AS CRC ON STL.NUMERO_INSTITUCION = CRC.NUMERO_INSTITUCION ");
                        query.append("  AND STL.NUMERO_GARANTIA = CRC.FOLIO_GARANTIA ");
                    }
                }
                if (numeroLayout == 2 /*|| numeroLayout == 14*/) {
                    query.append("LEFT JOIN V_PCLI_DATOS_CLIENTE AS VPDC ON VSC.NUMERO_INSTITUCION = VPDC.NUMERO_INSTITUCION ");
                    query.append("  AND VSC.NUMERO_CLIENTE = VPDC.NUMERO_CLIENTE ");
                } else {
                    query.append("LEFT JOIN V_PCLI_DATOS_CLIENTE AS VPDC ON CRC.NUMERO_INSTITUCION = VPDC.NUMERO_INSTITUCION ");
                    query.append("  AND CRC.NUMERO_CLIENTE = VPDC.NUMERO_CLIENTE ");
                }
            }
            /*if(numeroLayout == 19)
            {
            	query.append("LEFT JOIN V_PCLI_DATOS_CLIENTE AS VPDC ON STL.NUMERO_INSTITUCION = VPDC.NUMERO_INSTITUCION ");
            	query.append("  AND STL.NUMERO_CLIENTE = VPDC.NUMERO_CLIENTE ");
            }*/
            query.append("INNER JOIN SICC_CATALOGO_LAYOUT AS SCL ON STL.NUMERO_INSTITUCION = STL.NUMERO_INSTITUCION ");
            query.append("  AND STL.NUMERO_LAYOUT = SCL.NUMERO_LAYOUT ");
            query.append("LEFT JOIN SICC_CARGA_ERRORES AS SCE ON STL.NUMERO_INSTITUCION = SCE.NUMERO_INSTITUCION ");
            query.append("  AND STL.FOLIO_LOTE = SCE.NUMERO_FOLIO ");
            query.append("  AND STL.NUMERO_LAYOUT = SCE.NUMERO_LAYOUT ");
            query.append("  AND STL.NUMERO_CONSECUTIVO = SCE.NUMERO_CONSECUTIVO ");
            query.append("WHERE STL.NUMERO_INSTITUCION = ").append(numeroInstitucion);
            query.append("  AND SCL.NUMERO_LAYOUT = ").append(numeroLayout);
            query.append("  AND STL.FOLIO_LOTE = '").append(folioLote).append("'");
            System.out.println(query);
            bd.executeUpdate(query.toString());

            bd.commit();
            bd.close();
        } catch (SQLException se) {
            reportesDEPP.borraTabla(numeroInstitucion, idUsuario, fechaReporte, claveReporte, bd);
            bd.close();

            se.printStackTrace();
            MensajesSistema mensajesSistema = new MensajesSistema();
            throw new SQLException(mensajesSistema.getMensaje(131));
        }
    }

    public String getReporte() throws Exception {
        String nombreReporte = null;

        try {

            Map<String, String> hmp = new HashMap<>();
            Map<String, String> hmpFiltros = new HashMap<>();
            Map<String, String> hmpList = new HashMap<>();
            numeroInstitucion = institucion.getNumeroInstitucion(serialNumber);

            getNombreLayout();
            nombreReporte = "CARGA_" + nombreLayout + "_" + folioLote;

            // ARCHIVO PDF
            hmp.put("SERIAL_NUMBER", serialNumber);
            hmp.put("ID_USUARIO", idUsuario);
            hmp.put("CLAVE_FUNCION", nombreReporte);//nombre archivo
            hmp.put("NOMBRE_REPORTE", "Validacion_Carga_ArchivoDatosFaltantesPDF");
            hmp.put("TIPO_REPORTE", "0");

            hmpFiltros.put("CLAVE_REPORTE", claveReporte);
            //hmpFiltros.put("FECHA_SISTEMA", fechaReporte);
            if (numeroLayout != 4 && numeroLayout != 3) {
                hmpFiltros.put("GET_CAMPO", "Numero Cuenta");
            } else {
                if (numeroLayout == 3) {
                    hmpFiltros.put("GET_CAMPO", "Numero Garantia");
                } else if (numeroLayout == 4) {
                    hmpFiltros.put("GET_CAMPO", "Numero Linea");
                }
            }

            hmpFiltros.put("NOMBRE_INSTITUCION", institucion.getNombreComercialInstitucion(serialNumber));
            hmpFiltros.put("NUMERO_INSTITUCION", Short.toString(numeroInstitucion));
            hmpFiltros.put("ID_USUARIO", idUsuario);
//            hmpFiltros.put("FECHA_GENERACION", fechaReporte);
            hmpFiltros.put("FECHA_GENERACION", fechaReporte.substring(0, 10));

            System.out.println("-------------------creacion archivo PDF-------------------------");
            Reporte reporte = new Reporte(hmp, hmpFiltros, hmpList, pathReporte);
            reporte.creaCarpeta();
            reporte.creaReporte();

            System.out.println("-------------------creacion archivo CSV-------------------------");
            getConsulta();
            reporte = new Reporte(hmp, hmpFiltros, hmpList, pathReporte);
            reporte.creaCarpeta();
            pathReporte = reporte.getPathReporte();
            Date date = new Date();
            SQLProperties sqlproperties = new SQLProperties();
            SimpleDateFormat formato_hora = new SimpleDateFormat(sqlproperties.getformatoReporteHora());
            String cadenaHora = formato_hora.format(date);
            FileUtils.writeLines(new File(pathReporte + File.separator + nombreReporte + "_" + cadenaHora + ".csv"), "ISO-8859-1", listData);

            bd = new JDBCConnectionPool();
            reportesDEPP.borraTabla(numeroInstitucion, idUsuario, fechaReporte, claveReporte, bd);
            bd.close();
        } catch (Exception se) {
            reportesDEPP.borraTabla(numeroInstitucion, idUsuario, fechaReporte, claveReporte, bd);
            if (bd != null) {
                bd.close();
            }
            se.printStackTrace();
            MensajesSistema mensajesSistema = new MensajesSistema();
            throw new SQLException(mensajesSistema.getMensaje(131));
        }
        return nombreReporte;
    }

    public void getConsulta() throws Exception {
        MensajesSistema msjSistema = new MensajesSistema();
        sqlProperties = new SQLProperties();

        try {
            bd = new JDBCConnectionPool();

            query = new StringBuffer();
            query.append("SELECT REP.NUMERO_INSTITUCION ");
            query.append("  ,REPLACE(GI.NOMBRE_COMERCIAL, ',', '') AS NOMBRE_INSTITUCION ");
            query.append("  ,REP.NUMERO_LAYOUT ");
            query.append("  ,REP.NOMBRE_LAYOUT ");
            query.append("  ,REP.NUMERO_FOLIO ");
            query.append("  ,REP.NOMBRE_ARCHIVO ");
            query.append("  ,REP.CLAVE_REPORTE ");
            query.append("  ,REP.FECHA_GENERACION AS FECHA_PROCESO ");
            query.append("  ,REP.NUMERO_CONSECUTIVO ");
            query.append("  ,REP.NUMERO_CLIENTE ");
            query.append("  ,CASE  ");
            query.append("    WHEN REP.NUMERO_CUENTA IS NOT NULL ");
            query.append("      THEN REP.NUMERO_CUENTA ");
            query.append("    ELSE REP.NUMERO_LINEA ");
            query.append("    END CUENTA_LINEA ");
            query.append("  ,REP.NUMERO_CUENTA ");
            query.append("  ,REP.NUMERO_LINEA ");
            query.append("  ,REP.STATUS_CARGA ");
            query.append("  ,REP.STATUS_ALTA ");
            query.append("  ,REP.CODIGO_MENSAJE ");
            query.append("  ,REPLACE(REP.DESCRIPCION_MENSAJE, ',', '') AS DESCRIPCION_MENSAJE ");
            query.append("  ,REP.ID_USUARIO ");
            query.append("FROM REP_435_DETALLE REP ");
            query.append("INNER JOIN GRAL_INSTITUCIONES GI ON GI.NUMERO_INSTITUCION = REP.NUMERO_INSTITUCION ");
            query.append("WHERE REP.NUMERO_INSTITUCION = ").append(numeroInstitucion);
            query.append("  AND REP.NUMERO_FOLIO = '").append(folioLote).append("'");
            System.out.println(query);
            resultado = bd.executeQuery(query.toString());

            listData = sqlProperties.getColumnName(resultado, ",");
            listData = sqlProperties.getColumnValueFormatoFecha(resultado, listData, sqlProperties.getFormatoSoloFechaOrion(), ",");

            bd.close();

        } catch (SQLException e) {
            bd.close();
            e.printStackTrace();
            throw new Exception(msjSistema.getMensaje(131));
        }

    }

    //4922
    public List<String> consultaCamposLayout(short numeroInstitucion, String numeroLayout) throws Exception {

        sqlProperties = new SQLProperties();

        try {

            bd = new JDBCConnectionPool();
            query = new StringBuffer("");
            query.append("SELECT IND_ACTUALIZA ");
            query.append("  ,NUMERO_CAMPO ");
            query.append("  ,NOMBRE_CAMPO ");
            query.append("  ,STATUS ");
            query.append("FROM SICC_CAMPOS_LAYOUT ");
            query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
            query.append("  AND NUMERO_LAYOUT = ").append(numeroLayout);
            System.out.println(query);
            resultado = bd.executeQuery(query.toString());

            listData = sqlProperties.getColumnName(resultado);
            listData = sqlProperties.getColumnValue(resultado, listData);

            bd.close();
        } catch (Exception e) {
            bd.close();
            e.printStackTrace();
            MensajesSistema mensajesSistema = new MensajesSistema();
            throw new SQLException(mensajesSistema.getMensaje(131));
        }
        return listData;
    }
    public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
    	ReporteCargaMasivaSicc rp = new ReporteCargaMasivaSicc("1","USRCONFIG","SCSD31102023_008", "C:s", "435",19);
	        rp.creaTabla();
	        rp.getReporte();
	}
    
}

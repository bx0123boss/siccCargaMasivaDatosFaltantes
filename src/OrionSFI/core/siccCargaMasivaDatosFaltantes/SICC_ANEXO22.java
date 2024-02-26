/*
 *********************************************************************************************************************
 *    FECHA         ID.REQUERIMIENTO            DESCRIPCIÓN                            MODIFICÓ.          
 * 09/02/2015             3226            Creación del documento con            José Manuel Zúñiga Flores 
 *                                        especificaciones de la Carga de       Fernando Vázquez Galán    
 *                                        Archivo para Datos Faltantes de                                 
 *                                        Calificación de Cartera                                         
 ********************************************************************************************************************* 
 *20/08/2015	4026	Cambio hecho por bajaware, modificacion a la funcionalidad de		José Manuel
 *			Sicc Persona (manual y masiva) para el dato Es Fondo ya que ahora 
 *			es un catálogo.
 *			Modificacion a la funcionalidad de garantías, para que en el combo 
 *			de localidad se despliegue el catalogo utilizado por bajaware.
 *********************************************************************************************************************
 */
package OrionSFI.core.siccCargaMasivaDatosFaltantes;

import OrionSFI.core.commons.InspectorDatos;
import OrionSFI.core.commons.JDBCConnectionPool;
import OrionSFI.core.commons.MensajesSistema;
import OrionSFI.core.commons.SQLProperties;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

public class SICC_ANEXO22 {

    private JDBCConnectionPool bd = null;
    private StringBuffer query = null;
    private ValidaCampos validaCampos = null;
    private static int dosDecimales = 2;
    private static int cuatroDecimales = 4;
    private static int seisDecimales = 6;

    /**
     * Valida cada registro.
     *
     * @param registro
     * @return
     * @throws Exception
     */
    public boolean validaRegistro(String registro) throws Exception {
        boolean valido = false;

        StringBuffer expRegular = new StringBuffer();
        //expRegular.append("(\\d{1,5}+\\|)");                                      //NUMERO_CONSECUTIVO
        expRegular.append("(\\d{1,15}+\\|)");                                     //NUMERO_CLIENTE
        expRegular.append("(\\d{0,8}+\\|)");                     				//PROMEDIO_DIAS_MORA_INSTBANC
        expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)");                      //PORCENTAJE_PAGOS_TIEMPO_INSTBANC
        expRegular.append("(\\d{0,8}+\\|)");                                      //NUM_INST_REP_ULT12M
        expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)");                      //PORCENTAJE_PAGOS_TIEMPO_NOBANC
        expRegular.append("(\\d{0,21}+\\||\\d{0,21}+\\.+\\d{0,2}+\\|)");                     //APORTACIONES_INFONAVIT_ULT_BIMESTRE
        expRegular.append("([a-zA-Z0-9-_ ]{0,8}+\\|)");                                    //DIAS_ATRASO_INFONAVIT_ULT_BIMESTRE
        expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)");                      //TASA_RETENSION_LABORAL
        expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)");                     //INDICADOR_ESTABILIDAD_ECONOMICA
        expRegular.append("(\\d{1,1}+\\|)");                                      //INT_CARACT_COMPETENCIA
        expRegular.append("(\\d{1,1}+\\|)");                                      //PROVEEDORES
        expRegular.append("(\\d{1,1}+\\|)");                                      //CLIENTES
        expRegular.append("(\\d{1,1}+\\|)");                                      //EDOS_FINANCIEROS_AUDITADOS
        expRegular.append("(\\d{1,1}+\\|)");                                      //NUMERO_AGENCIAS_CALIF
        expRegular.append("(\\d{1,1}+\\|)");                                      //INDEPENDECIA_CONSEJO_ADMON
        expRegular.append("(\\d{1,1}+\\|)");                                      //ESTRUCTURA_ORGANIZACIONAL
        expRegular.append("(\\d{1,1}+\\|)");                                      //COMPOSICION_ACCIONARIA
        expRegular.append("(\\d{0,21}+\\||\\d{0,21}+\\.+\\d{0,2}+\\|)");                     //GASTOS_FINANCIEROS
        expRegular.append("(\\d{0,21}+\\||\\d{0,21}+\\.+\\d{0,2}+\\|)");                     //PASIVO_CIRCULANTE
        expRegular.append("(\\d{0,21}+\\||\\d{0,21}+\\.+\\d{0,2}+\\|)");                     //ACTIVO_CIRCULANTE
        expRegular.append("(\\d{0,21}+\\||\\d{0,21}+\\.+\\d{0,2}+\\|)");                     //ACTIVO_TOTAL_ANUAL
        expRegular.append("(-?\\d{0,21}+\\||-?\\d{0,21}+\\.+\\d{0,2}+\\|)");                     //CAPITAL_CONTABLE_PROMEDIO
        expRegular.append("(-?\\d{0,21}+\\||-?\\d{0,21}+\\.+\\d{0,2}+\\|)");                     //UTILIDAD_NETA
        expRegular.append("(-?\\d{0,21}+\\||-?\\d{0,21}+\\.+\\d{0,2}+\\|)");                     //UTILIDAD_ANT_FIN_IMPTO
        expRegular.append("((0|1){1,1}+\\|)");                                    //ES_ORGANISMO_DESC_PART_POLITICO
        expRegular.append("((0?[1-9]|[12][0-9]|3[01])?(/|-)?(0?[1-9]|1[012])?(/|-)?((19|20)\\d\\d)?+\\|)");//FECHA_INFORMACION_FINANCIERA
        expRegular.append("((0?[1-9]|[12][0-9]|3[01])?(/|-)?(0?[1-9]|1[012])?(/|-)?((19|20)\\d\\d)?+\\|)");//FECHA_INFORMACION_BURO
        expRegular.append("((0|1){1,1}+\\|)");                                    //CALIFICACION_AGENCIA_CALIF
        expRegular.append("((0|1){1,1}+\\|)");                                    //EXPERIENCIA_NEGATIVA_PAGO
        expRegular.append("(\\d{0,1}+\\|)");                                      //ES_GARANTE
        expRegular.append("((0|1){1,1})");                                        //SIN_ATRASOS

        if (registro.toString().matches(expRegular.toString())) {
            valido = true;
        }

        return valido;
    }

    public String separaRegistros(String registro, int consecutivo, int noLayout, String folioLote, short numeroInstitucion, String idUsuario, String macAddress) throws Exception {
        validaCampos = new ValidaCampos(numeroInstitucion, idUsuario, macAddress, folioLote);
        InspectorDatos iData = new InspectorDatos();
        StringBuffer errRegistro = new StringBuffer();
        StringTokenizer st = new StringTokenizer(registro, "|");
        char pipe = '|';
        String[] cad = registro.split("\\|");

        errRegistro.setLength(0);
        errRegistro.append(consecutivo).append(pipe);
     
          if (iData.isStringNULLExt(cad[0])) {        	  
            if (cad[0].equalsIgnoreCase("NULL")){
                errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NUMERO_CLIENTE", 15, noLayout, consecutivo, (byte) 1)).append(pipe);
            }else{
                    errRegistro.append(validaCampos.getCampoNumerico(cad[0], "NUMERO_CLIENTE", 15, noLayout, consecutivo, (byte) 1)).append(pipe);
            	}
          } else {
            errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NUMERO_CLIENTE", 15, noLayout, consecutivo, (byte) 1)).append(pipe);
          	}

        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "PROMEDIO_DIAS_MORA_INSTBANC", 8, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PORCENTAJE_PAGOS_TIEMPO_INSTBANC", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NUM_INST_REP_ULT12M", 8, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PORCENTAJE_PAGOS_TIEMPO_NOBANC", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "APORTACIONES_INFONAVIT_ULT_BIMESTRE", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "DIAS_ATRASO_INFONAVIT_ULT_BIMESTRE", 8, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "TASA_RETENSION_LABORAL", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "INDICADOR_ESTABILIDAD_ECONOMICA", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "INT_CARACT_COMPETENCIA", 1, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "PROVEEDORES", 1, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "CLIENTES", 1, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "EDOS_FINANCIEROS_AUDITADOS", 1, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NUMERO_AGENCIAS_CALIF", 1, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "INDEPENDECIA_CONSEJO_ADMON", 1, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "ESTRUCTURA_ORGANIZACIONAL", 1, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "COMPOSICION_ACCIONARIA", 1, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "GASTOS_FINANCIEROS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PASIVO_CIRCULANTE", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "ACTIVO_CIRCULANTE", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "ACTIVO_TOTAL_ANUAL", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimalesNegativo(st.nextToken(), "CAPITAL_CONTABLE_PROMEDIO", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimalesNegativo(st.nextToken(), "UTILIDAD_NETA", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimalesNegativo(st.nextToken(), "UTILIDAD_ANT_FIN_IMPTO", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "ES_ORGANISMO_DESC_PART_POLITICO", 1, noLayout, consecutivo, "0", (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "FECHA_INFORMACION_FINANCIERA", 10, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "FECHA_INFORMACION_BURO", 10, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "CALIFICACION_AGENCIA_CALIF", 1, noLayout, consecutivo, "0", (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "EXPERIENCIA_NEGATIVA_PAGO", 1, noLayout, consecutivo, "0", (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "ES_GARANTE", 1, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "SIN_ATRASOS", 1, noLayout, consecutivo, "0", (byte) 1)).append(pipe);

        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "VENTAS_NETAS_INGRESOS", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "POR_PAGO_TIEMPO_INSTCRED", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "UTILIDAD_NETA_ANUAL", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "CAPITAL_CONTABLE", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "CUENTASXCOBRAR", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "INGRESOSANUALES", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "EFECTIVO", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "ACTIVOTOTAL", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "UTILIDADOPERACION", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "GASTOSINTERESES", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "INVCORTOPLAZO", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PROPPLANTAEQUIPO", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PROVEEDORES_2", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PRESTAMOSCP", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "COSTOVENTAS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "ACTIVOCIRCULANTE", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "UTILIDADBRUTA", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PASIVOTOTAL", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "IND_EST_ECONOMICA", 1, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "INT_CAR_COMPETENCIA", 1, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "DOMICILIO_EXT", 1, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PI", 16, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "PUNTAJE_TOTAL", 6, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "PUNTAJE_CUANT", 6, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "PUNTAJE_CUAL", 6, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "ALFA", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "MESES_PI_100", 6, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_DIAS_MORA_P12M", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_POR_PAG_TIEM_ENT_NO_BAN", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_MAX_ATR_7M", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_PROM_DIAS_MORA", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "POR_SDO_SIN_ATR_4M", 16, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "ROE", 16, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PER_COBRO_DEUD", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "R_EF_ACTIVO", 16, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "R_COB_INT", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "R_EFECTIVO", 16, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "R_USO_ACT_FIJO", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "R_VENTAS_CAP", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "R_FIN_A_VTAS", 16, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "R_ROT_ACT", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PER_PAGO_PROVEED", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "P_CAP_TRAB_VENTAS", 16, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "MARGEN_BRUTO", 16, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "R_SOBRE _CAPITAL_ROE", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "PERIODO_COBRO_DEUDORES", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "R_EFECTIVO_ACTIVO_TOTAL", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "R_COBERTURA_INTERES", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_RAZON_EFECTIVO", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "R_USO _ACTIVOS_FIJOS", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "R_VENTAS_CAPITAL_OPERATIVO_EMPLEADO", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "R_COSTO_FINANCIAMIENTO_VENTAS", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "ROTACION_ACTIVOS_TOTALES", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "PERIODO_PAGO_ACREEDORES", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "R_CAPITAL _TRABAJO_VENTAS", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "MARGEN_BRUTO_UTILIDAD", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "FECHA_INFO", 10, noLayout, consecutivo, (byte) 1)).append(pipe);

        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Máximo Atrasos Ultimos 7M", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        
        return errRegistro.toString();
    }

    public void insertaCarga(String registro, short numeroInstitucion, String nombreArchivo, String folioLote, int noLayout, String idUsuario, String macAddress) throws Exception {
        MensajesSistema msjSistema = new MensajesSistema();
        validaCampos = new ValidaCampos();
        StringTokenizer st = new StringTokenizer(registro, "|");
        System.out.println(st.countTokens());
        byte numero = 0;
        byte cadena = 1;

        SQLProperties sqlProperties = new SQLProperties();
        SimpleDateFormat sdf = new SimpleDateFormat(sqlProperties.getFormatoSoloFechaOrion());
        String fechaAlta = sqlProperties.getFechaInsercionFormateada(sdf.format(new Date()));
        InspectorDatos iDat = new InspectorDatos();
        try {
            bd = new JDBCConnectionPool();
            bd.setAutoCommit(false);

            query = new StringBuffer();
            query.append("INSERT INTO dbo.SICC_ANEXO22_LO(");
            query.append("  NUMERO_INSTITUCION");
            query.append(", NOMBRE_ARCHIVO");
            query.append(", FOLIO_LOTE");
            query.append(", NUMERO_LAYOUT");
            query.append(", NUMERO_CONSECUTIVO");
            query.append(", NUMERO_CLIENTE");
            query.append(", PROMEDIO_DIAS_MORA_INSTBANC");
            query.append(", PORCENTAJE_PAGOS_TIEMPO_INSTBANC");
            query.append(", NUM_INST_REP_ULT12M");
            query.append(", PORCENTAJE_PAGOS_TIEMPO_NOBANC");
            query.append(", APORTACIONES_INFONAVIT_ULT_BIMESTRE");
            query.append(", DIAS_ATRASO_INFONAVIT_ULT_BIMESTRE");
            query.append(", TASA_RETENSION_LABORAL");
            query.append(", INDICADOR_ESTABILIDAD_ECONOMICA");
            query.append(", INT_CARACT_COMPETENCIA");
            query.append(", PROVEEDORES");
            query.append(", CLIENTES");
            query.append(", EDOS_FINANCIEROS_AUDITADOS");
            query.append(", NUMERO_AGENCIAS_CALIF");
            query.append(", INDEPENDECIA_CONSEJO_ADMON");
            query.append(", ESTRUCTURA_ORGANIZACIONAL");
            query.append(", COMPOSICION_ACCIONARIA");
            query.append(", GASTOS_FINANCIEROS");
            query.append(", PASIVO_CIRCULANTE");
            query.append(", ACTIVO_CIRCULANTE");
            query.append(", ACTIVO_TOTAL_ANUAL");
            query.append(", CAPITAL_CONTABLE_PROMEDIO");
            query.append(", UTILIDAD_NETA");
            query.append(", UTILIDAD_ANT_FIN_IMPTO");
            query.append(", ES_ORGANISMO_DESC_PART_POLITICO");
            query.append(", FECHA_INFORMACION_FINANCIERA");
            query.append(", FECHA_INFORMACION_BURO");
            query.append(", CALIFICACION_AGENCIA_CALIF");
            query.append(", EXPERIENCIA_NEGATIVA_PAGO");
            query.append(", ES_GARANTE");
            query.append(", SIN_ATRASOS");
            
            query.append(",VENTAS_NETAS_INGRESOS");
            query.append(",POR_PAGO_TIEMPO_INSTCRED");
            query.append(",UTILIDAD_NETA_ANUAL");
            query.append(",CAPITAL_CONTABLE");
            query.append(",CUENTASXCOBRAR");
            query.append(",INGRESOSANUALES");
            query.append(",EFECTIVO");
            query.append(",ACTIVOTOTAL");
            query.append(",UTILIDADOPERACION");
            query.append(",GASTOSINTERESES");
            query.append(",INVCORTOPLAZO");
            query.append(",PROPPLANTAEQUIPO");
            query.append(",PROVEEDORES_2");
            query.append(",PRESTAMOSCP");
            query.append(",COSTOVENTAS");
            query.append(",ACTIVOCIRCULANTE");
            query.append(",UTILIDADBRUTA");
            query.append(",PASIVOTOTAL");
            query.append(",IND_EST_ECONOMICA");
            query.append(",INT_CAR_COMPETENCIA");
            query.append(",DOMICILIO_EXT");
            query.append(",PI");
            query.append(",PUNTAJE_TOTAL");
            query.append(",PUNTAJE_CUANT");
            query.append(",PUNTAJE_CUAL");
            query.append(",ALFA");
            query.append(",MESES_PI_100");
            query.append(",P_DIAS_MORA_P12M");
            query.append(",P_POR_PAG_TIEM_ENT_NO_BAN");
            query.append(",P_MAX_ATR_7M");
            query.append(",P_PROM_DIAS_MORA");
            query.append(",POR_SDO_SIN_ATR_4M");
            query.append(",ROE");
            query.append(",PER_COBRO_DEUD");
            query.append(",R_EF_ACTIVO");
            query.append(",R_COB_INT");
            query.append(",R_EFECTIVO");
            query.append(",R_USO_ACT_FIJO");
            query.append(",R_VENTAS_CAP");
            query.append(",R_FIN_A_VTAS");
            query.append(",R_ROT_ACT");
            query.append(",PER_PAGO_PROVEED");
            query.append(",P_CAP_TRAB_VENTAS");
            query.append(",MARGEN_BRUTO");
            query.append(",R_SOBRE_CAPITAL_ROE");
            query.append(",PERIODO_COBRO_DEUDORES");
            query.append(",R_EFECTIVO_ACTIVO_TOTAL");
            query.append(",R_COBERTURA_INTERES");
            query.append(",P_RAZON_EFECTIVO");
            query.append(",R_USO_ACTIVOS_FIJOS");
            query.append(",R_VENTAS_CAPITAL_OPERATIVO_EMPLEADO");
            query.append(",R_COSTO_FINANCIAMIENTO_VENTAS");
            query.append(",ROTACION_ACTIVOS_TOTALES");
            query.append(",PERIODO_PAGO_ACREEDORES");
            query.append(",R_CAPITAL_TRABAJO_VENTAS");
            query.append(",MARGEN_BRUTO_UTILIDAD");
            query.append(",FECHA_INFO");
            
            query.append("  ,MAXATR");
            
            query.append(", STATUS_CARGA");
            query.append(", STATUS_ALTA");
            query.append(", STATUS");
            query.append(", FECHA_ALTA");
            query.append(", ID_USUARIO_ALTA");
            query.append(", MAC_ADDRESS_ALTA");
            query.append(", FECHA_MODIFICACION");
            query.append(", ID_USUARIO_MODIFICACION");
            query.append(", MAC_ADDRESS_MODIFICACION) ");
            query.append("VALUES(");
            query.append("  ").append(numeroInstitucion);
            query.append(", '").append(nombreArchivo).append("'");
            query.append(", '").append(folioLote).append("'");
            query.append(", ").append(noLayout);
            query.append(", ").append(st.nextToken());                              //NUMERO_CONSECUTIVO
            query.append(", ").append(st.nextToken());                              //NUMERO_CLIENTE
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//PROMEDIO_DIAS_MORA_INSTBANC
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//PORCENTAJE_PAGOS_TIEMPO_INSTBANC
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//NUM_INST_REP_ULT12M
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//PORCENTAJE_PAGOS_TIEMPO_NOBANC
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//APORTACIONES_INFONAVIT_ULT_BIMESTRE
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), cadena));//DIAS_ATRASO_INFONAVIT_ULT_BIMESTRE
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//TASA_RETENSION_LABORAL
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//INDICADOR_ESTABILIDAD_ECONOMICA
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//INT_CARACT_COMPETENCIA
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//PROVEEDORES
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//CLIENTES
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//EDOS_FINANCIEROS_AUDITADOS
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//NUMERO_AGENCIAS_CALIF
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//INDEPENDECIA_CONSEJO_ADMON
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//ESTRUCTURA_ORGANIZACIONAL
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//COMPOSICION_ACCIONARIA
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//GASTOS_FINANCIEROS
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//PASIVO_CIRCULANTE
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//ACTIVO_CIRCULANTE
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//ACTIVO_TOTAL_ANUAL
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//CAPITAL_CONTABLE_PROMEDIO
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//UTILIDAD_NETA
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//UTILIDAD_ANT_FIN_IMPTO
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//ES_ORGANISMO_DESC_PART_POLITICO
            
           /* query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), cadena));//FECHA_INFORMACION_FINANCIERA
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), cadena));//FECHA_INFORMACION_BURO*/
            
            String fechaInformacionFinanciera=st.nextToken();
            if(iDat.isStringNULLExt(fechaInformacionFinanciera))
            	query.append("  ,NULL");
            else
            	query.append("  ,'").append(fechaInformacionFinanciera).append("'");						//FECHA_INFORMACION_FINANCIERA
            
            String fechaInformacionBuro = st.nextToken();
            if(iDat.isStringNULLExt(fechaInformacionBuro))
            	query.append("  ,NULL");
            else
            	query.append("  ,'").append(fechaInformacionBuro).append("'");                    //FECHA_INFORMACION_BURO
            
            
            
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//CALIFICACION_AGENCIA_CALIF
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//EXPERIENCIA_NEGATIVA_PAGO
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//ES_GARANTE
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//SIN_ATRASOS
            
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	VENTAS_NETAS_INGRESOS
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	POR_PAGO_TIEMPO_INSTCRED
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	UTILIDAD_NETA_ANUAL
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	CAPITAL_CONTABLE
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	CUENTASXCOBRAR
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	INGRESOSANUALES
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	EFECTIVO
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	ACTIVOTOTAL
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	UTILIDADOPERACION
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	GASTOSINTERESES
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	INVCORTOPLAZO
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	PROPPLANTAEQUIPO
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	PROVEEDORES_2
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	PRESTAMOSCP
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	COSTOVENTAS
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	ACTIVOCIRCULANTE
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	UTILIDADBRUTA
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	PASIVOTOTAL
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	IND_EST_ECONOMICA
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	INT_CAR_COMPETENCIA
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	DOMICILIO_EXT
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	PI
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	PUNTAJE_TOTAL
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	PUNTAJE_CUANT
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	PUNTAJE_CUAL
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	ALFA
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	MESES_PI_100
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	P_DIAS_MORA_P12M
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	P_POR_PAG_TIEM_ENT_NO_BAN
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	P_MAX_ATR_7M
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	P_PROM_DIAS_MORA
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	POR_SDO_SIN_ATR_4M
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	ROE
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	PER_COBRO_DEUD
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	R_EF_ACTIVO
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	R_COB_INT
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	R_EFECTIVO
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	R_USO_ACT_FIJO
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	R_VENTAS_CAP
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	R_FIN_A_VTAS
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	R_ROT_ACT
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	PER_PAGO_PROVEED
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	P_CAP_TRAB_VENTAS
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	MARGEN_BRUTO
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	R_SOBRE _CAPITAL_ROE
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	PERIODO_COBRO_DEUDORES
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	R_EFECTIVO_ACTIVO_TOTAL
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	R_COBERTURA_INTERES
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	P_RAZON_EFECTIVO
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	R_USO _ACTIVOS_FIJOS
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	R_VENTAS_CAPITAL_OPERATIVO_EMPLEADO
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	R_COSTO_FINANCIAMIENTO_VENTAS
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	ROTACION_ACTIVOS_TOTALES
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	PERIODO_PAGO_ACREEDORES
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	R_CAPITAL _TRABAJO_VENTAS
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	MARGEN_BRUTO_UTILIDAD
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), cadena));	//	FECHA_INFO
            query.append(", ").append(st.nextToken()); 
            
            query.append(", 'NP'");
            query.append(", 'NP'");
            query.append(", 1");
            query.append(", '").append(fechaAlta).append("'");
            query.append(", '").append(idUsuario).append("'");
            query.append(", '").append(macAddress).append("'");
            query.append(", NULL");
            query.append(", NULL");
            query.append(", NULL)");
            System.out.println(query);
            bd.executeStatement(query.toString());

            bd.commit();
            bd.close();
        } catch (SQLException e) {
            bd.rollback();
            bd.close();
            e.printStackTrace();
            throw new Exception(msjSistema.getMensaje(131));
        }
    }

    public void procesoAltaModificacion(short numeroInstitucion, int noLayout, String fechaProceso, String folioLote, String idUsuario, String macAddress) throws Exception {
        MensajesSistema msjSistema = new MensajesSistema();
        SQLProperties sqlProperties = new SQLProperties();
        UpdateCommon updateCommon = new UpdateCommon();
        List<String> listCampos = new ArrayList<>();
        List<String> listDatos = new ArrayList<>();
        ResultSet resultadoCampos;
        ResultSet resultadoDatos;
        int consecutivo = 0;

        try {
            bd = new JDBCConnectionPool();

            query = new StringBuffer("");
            query.append("SELECT UPPER(ISC.DATA_TYPE) AS TIPO_DATO ");
            query.append("  ,ISC.COLUMN_NAME AS CAMPO ");
            query.append("FROM SICC_CAMPOS_LAYOUT AS SCL ");
            query.append("INNER JOIN INFORMATION_SCHEMA.COLUMNS ISC ON ISC.COLUMN_NAME = SCL.NOMBRE_CAMPO ");
            query.append("  AND ISC.TABLE_NAME = 'SICC_FALTANTES_ANEXO22' ");
            query.append("  AND SCL.IND_ACTUALIZA = 1 ");
            query.append("  AND SCL.NUMERO_INSTITUCION = ").append(numeroInstitucion);
            query.append("  AND SCL.NUMERO_LAYOUT = ").append(noLayout);
            System.out.println(query);
            resultadoCampos = bd.executeQuery(query.toString());
            listCampos = sqlProperties.getColumnValue(resultadoCampos, listCampos);
            String campos;
            int i = 0;

            query.setLength(0);
            query.append("SELECT STL.NUMERO_CLIENTE ");
            query.append("  ,STL.NUMERO_CONSECUTIVO ");
            while (i < listCampos.size()) {
                campos = listCampos.get(i).toString();
                campos = campos.substring(campos.indexOf("&#") + 2, campos.length());
                query.append("  ,STL.").append(campos);
                i++;
            }
            query.append(" FROM SICC_ANEXO22_LO AS STL ");
            query.append(" WHERE STL.NUMERO_CLIENTE IS NOT NULL ");
            query.append("   AND EXISTS ( ");
            query.append("     SELECT SFT.NUMERO_CLIENTE ");
            query.append("     FROM SICC_FALTANTES_ANEXO22 AS SFT ");
            query.append("     WHERE STL.NUMERO_INSTITUCION = SFT.NUMERO_INSTITUCION ");
            query.append("       AND STL.NUMERO_CLIENTE = SFT.NUMERO_CLIENTE ");
            query.append("     ) ");
            query.append("   AND STL.NUMERO_INSTITUCION = ").append(numeroInstitucion);
            query.append("   AND STL.STATUS_CARGA = 'P' ");
            query.append("   AND STL.FOLIO_LOTE = '").append(folioLote).append("'");
            System.out.println(query);
            resultadoDatos = bd.executeQuery(query.toString());
            listDatos = sqlProperties.getColumnValueFormatoFecha(resultadoDatos, listDatos, sqlProperties.getFormatoSoloFechaOrion());

            /*
             * SP ALTA_DATOS_FALTANTES_SICC
             */
            try {
                bd.setAutoCommit(false);
                System.out.println("--------------------------------------------ALTA_DATOS_FALTANTES_SICC--------------------------------------------");
                query.setLength(0);
                query.append("ALTA_DATOS_FALTANTES_SICC(");
                query.append(numeroInstitucion);
                query.append(", ").append(noLayout);
                query.append(", '").append(fechaProceso).append("'");
                query.append(", '").append(folioLote).append("'");
                query.append(", '").append(idUsuario).append("'");
                query.append(", '").append(macAddress).append("')");
                System.out.println(query);
                bd.executeStoreProcedure(query.toString());

                bd.commit();
            } catch (SQLException e) {
                bd.rollback();
                bd.close();
                e.printStackTrace();
                throw new Exception(msjSistema.getMensaje(131));
            }

            i = 0;
            Date fechaActual = new Date();
            SimpleDateFormat formato = new SimpleDateFormat(sqlProperties.getFormatoFecha());
            String fechaModificacion = formato.format(fechaActual);

            while (i < listDatos.size()) {
                try {
                    StringTokenizer stD = new StringTokenizer(listDatos.get(i).toString(), "&#");
                    String numeroCliente = stD.nextToken();
                    consecutivo = Integer.parseInt(stD.nextToken());
                    bd.setAutoCommit(false);
                    query.setLength(0);
                    query.append("INSERT INTO LOG_SICC_FALTANTES_ANEXO22 (");
                    query.append("  NUMERO_INSTITUCION ");
                    query.append("  ,NUMERO_CLIENTE ");
                    query.append("  ,FECHA_MODIFICACION ");
                    query.append("  ,PROMEDIO_DIAS_MORA_INSTBANC ");
                    query.append("  ,PORCENTAJE_PAGOS_TIEMPO_INSTBANC ");
                    query.append("  ,NUM_INST_REP_ULT12M ");
                    query.append("  ,PORCENTAJE_PAGOS_TIEMPO_NOBANC ");
                    query.append("  ,APORTACIONES_INFONAVIT_ULT_BIMESTRE ");
                    query.append("  ,DIAS_ATRASO_INFONAVIT_ULT_BIMESTRE ");
                    query.append("  ,TASA_RETENSION_LABORAL ");
                    query.append("  ,INDICADOR_ESTABILIDAD_ECONOMICA ");
                    query.append("  ,INT_CARACT_COMPETENCIA ");
                    query.append("  ,PROVEEDORES ");
                    query.append("  ,CLIENTES ");
                    query.append("  ,EDOS_FINANCIEROS_AUDITADOS ");
                    query.append("  ,NUMERO_AGENCIAS_CALIF ");
                    query.append("  ,INDEPENDECIA_CONSEJO_ADMON ");
                    query.append("  ,ESTRUCTURA_ORGANIZACIONAL ");
                    query.append("  ,COMPOSICION_ACCIONARIA ");
                    query.append("  ,GASTOS_FINANCIEROS ");
                    query.append("  ,PASIVO_CIRCULANTE ");
                    query.append("  ,ACTIVO_CIRCULANTE ");
                    query.append("  ,ACTIVO_TOTAL_ANUAL ");
                    query.append("  ,CAPITAL_CONTABLE_PROMEDIO ");
                    query.append("  ,UTILIDAD_NETA ");
                    query.append("  ,UTILIDAD_ANT_FIN_IMPTO ");
                    query.append("  ,ES_ORGANISMO_DESC_PART_POLITICO ");
                    query.append("  ,FECHA_INFORMACION_FINANCIERA ");
                    query.append("  ,FECHA_INFORMACION_BURO ");
                    query.append("  ,CALIFICACION_AGENCIA_CALIF ");
                    query.append("  ,EXPERIENCIA_NEGATIVA_PAGO ");
                    query.append("  ,ES_GARANTE ");
                    query.append("  ,SIN_ATRASOS ");
                    
                    query.append(", VENTAS_NETAS_INGRESOS");
                    query.append(", POR_PAGO_TIEMPO_INSTCRED");
                    query.append(", UTILIDAD_NETA_ANUAL");
                    query.append(", CAPITAL_CONTABLE");
                    query.append(", CUENTASXCOBRAR");
                    query.append(", INGRESOSANUALES");
                    query.append(", EFECTIVO");
                    query.append(", ACTIVOTOTAL");
                    query.append(", UTILIDADOPERACION");
                    query.append(", GASTOSINTERESES");
                    query.append(", INVCORTOPLAZO");
                    query.append(", PROPPLANTAEQUIPO");
                    query.append(", PROVEEDORES_2");
                    query.append(", PRESTAMOSCP");
                    query.append(", COSTOVENTAS");
                    query.append(", ACTIVOCIRCULANTE");
                    query.append(", UTILIDADBRUTA");
                    query.append(", PASIVOTOTAL");
                    query.append(", IND_EST_ECONOMICA");
                    query.append(", INT_CAR_COMPETENCIA");
                    query.append(", DOMICILIO_EXT");
                    query.append(", PI");
                    query.append(", PUNTAJE_TOTAL");
                    query.append(", PUNTAJE_CUANT");
                    query.append(", PUNTAJE_CUAL");
                    query.append(", ALFA");
                    query.append(", MESES_PI_100");
                    query.append(", P_DIAS_MORA_P12M");
                    query.append(", P_POR_PAG_TIEM_ENT_NO_BAN");
                    query.append(", P_MAX_ATR_7M");
                    query.append(", P_PROM_DIAS_MORA");
                    query.append(", POR_SDO_SIN_ATR_4M");
                    query.append(", ROE");
                    query.append(", PER_COBRO_DEUD");
                    query.append(", R_EF_ACTIVO");
                    query.append(", R_COB_INT");
                    query.append(", R_EFECTIVO");
                    query.append(", R_USO_ACT_FIJO");
                    query.append(", R_VENTAS_CAP");
                    query.append(", R_FIN_A_VTAS");
                    query.append(", R_ROT_ACT");
                    query.append(", PER_PAGO_PROVEED");
                    query.append(", P_CAP_TRAB_VENTAS");
                    query.append(", MARGEN_BRUTO");
                    query.append(", R_SOBRE_CAPITAL_ROE");
                    query.append(", PERIODO_COBRO_DEUDORES");
                    query.append(", R_EFECTIVO_ACTIVO_TOTAL");
                    query.append(", R_COBERTURA_INTERES");
                    query.append(", P_RAZON_EFECTIVO");
                    query.append(", R_USO_ACTIVOS_FIJOS");
                    query.append(", R_VENTAS_CAPITAL_OPERATIVO_EMPLEADO");
                    query.append(", R_COSTO_FINANCIAMIENTO_VENTAS");
                    query.append(", ROTACION_ACTIVOS_TOTALES");
                    query.append(", PERIODO_PAGO_ACREEDORES");
                    query.append(", R_CAPITAL_TRABAJO_VENTAS");
                    query.append(", MARGEN_BRUTO_UTILIDAD");
                    query.append(", FECHA_INFO ");
                    
                    query.append("  ,MAXATR");
                    
                    query.append("  ,STATUS ");
                    query.append("  ,ID_USUARIO_MODIFICACION ");
                    query.append("  ,MAC_ADDRESS_MODIFICACION ");
                    query.append("  ) ");
                    query.append("SELECT NUMERO_INSTITUCION ");
                    query.append("  ,NUMERO_CLIENTE ");
                    query.append("  ,'").append(fechaModificacion).append("'");
                    query.append("  ,PROMEDIO_DIAS_MORA_INSTBANC ");
                    query.append("  ,PORCENTAJE_PAGOS_TIEMPO_INSTBANC ");
                    query.append("  ,NUM_INST_REP_ULT12M ");
                    query.append("  ,PORCENTAJE_PAGOS_TIEMPO_NOBANC ");
                    query.append("  ,APORTACIONES_INFONAVIT_ULT_BIMESTRE ");
                    query.append("  ,DIAS_ATRASO_INFONAVIT_ULT_BIMESTRE ");
                    query.append("  ,TASA_RETENSION_LABORAL ");
                    query.append("  ,INDICADOR_ESTABILIDAD_ECONOMICA ");
                    query.append("  ,INT_CARACT_COMPETENCIA ");
                    query.append("  ,PROVEEDORES ");
                    query.append("  ,CLIENTES ");
                    query.append("  ,EDOS_FINANCIEROS_AUDITADOS ");
                    query.append("  ,NUMERO_AGENCIAS_CALIF ");
                    query.append("  ,INDEPENDECIA_CONSEJO_ADMON ");
                    query.append("  ,ESTRUCTURA_ORGANIZACIONAL ");
                    query.append("  ,COMPOSICION_ACCIONARIA ");
                    query.append("  ,GASTOS_FINANCIEROS ");
                    query.append("  ,PASIVO_CIRCULANTE ");
                    query.append("  ,ACTIVO_CIRCULANTE ");
                    query.append("  ,ACTIVO_TOTAL_ANUAL ");
                    query.append("  ,CAPITAL_CONTABLE_PROMEDIO ");
                    query.append("  ,UTILIDAD_NETA ");
                    query.append("  ,UTILIDAD_ANT_FIN_IMPTO ");
                    query.append("  ,ES_ORGANISMO_DESC_PART_POLITICO ");
                    query.append("  ,FECHA_INFORMACION_FINANCIERA ");
                    query.append("  ,FECHA_INFORMACION_BURO ");
                    query.append("  ,CALIFICACION_AGENCIA_CALIF ");
                    query.append("  ,EXPERIENCIA_NEGATIVA_PAGO ");
                    query.append("  ,ES_GARANTE ");
                    query.append("  ,SIN_ATRASOS ");
                    
                    query.append(", VENTAS_NETAS_INGRESOS");
                    query.append(", POR_PAGO_TIEMPO_INSTCRED");
                    query.append(", UTILIDAD_NETA_ANUAL");
                    query.append(", CAPITAL_CONTABLE");
                    query.append(", CUENTASXCOBRAR");
                    query.append(", INGRESOSANUALES");
                    query.append(", EFECTIVO");
                    query.append(", ACTIVOTOTAL");
                    query.append(", UTILIDADOPERACION");
                    query.append(", GASTOSINTERESES");
                    query.append(", INVCORTOPLAZO");
                    query.append(", PROPPLANTAEQUIPO");
                    query.append(", PROVEEDORES_2");
                    query.append(", PRESTAMOSCP");
                    query.append(", COSTOVENTAS");
                    query.append(", ACTIVOCIRCULANTE");
                    query.append(", UTILIDADBRUTA");
                    query.append(", PASIVOTOTAL");
                    query.append(", IND_EST_ECONOMICA");
                    query.append(", INT_CAR_COMPETENCIA");
                    query.append(", DOMICILIO_EXT");
                    query.append(", PI");
                    query.append(", PUNTAJE_TOTAL");
                    query.append(", PUNTAJE_CUANT");
                    query.append(", PUNTAJE_CUAL");
                    query.append(", ALFA");
                    query.append(", MESES_PI_100");
                    query.append(", P_DIAS_MORA_P12M");
                    query.append(", P_POR_PAG_TIEM_ENT_NO_BAN");
                    query.append(", P_MAX_ATR_7M");
                    query.append(", P_PROM_DIAS_MORA");
                    query.append(", POR_SDO_SIN_ATR_4M");
                    query.append(", ROE");
                    query.append(", PER_COBRO_DEUD");
                    query.append(", R_EF_ACTIVO");
                    query.append(", R_COB_INT");
                    query.append(", R_EFECTIVO");
                    query.append(", R_USO_ACT_FIJO");
                    query.append(", R_VENTAS_CAP");
                    query.append(", R_FIN_A_VTAS");
                    query.append(", R_ROT_ACT");
                    query.append(", PER_PAGO_PROVEED");
                    query.append(", P_CAP_TRAB_VENTAS");
                    query.append(", MARGEN_BRUTO");
                    query.append(", R_SOBRE_CAPITAL_ROE");
                    query.append(", PERIODO_COBRO_DEUDORES");
                    query.append(", R_EFECTIVO_ACTIVO_TOTAL");
                    query.append(", R_COBERTURA_INTERES");
                    query.append(", P_RAZON_EFECTIVO");
                    query.append(", R_USO_ACTIVOS_FIJOS");
                    query.append(", R_VENTAS_CAPITAL_OPERATIVO_EMPLEADO");
                    query.append(", R_COSTO_FINANCIAMIENTO_VENTAS");
                    query.append(", ROTACION_ACTIVOS_TOTALES");
                    query.append(", PERIODO_PAGO_ACREEDORES");
                    query.append(", R_CAPITAL_TRABAJO_VENTAS");
                    query.append(", MARGEN_BRUTO_UTILIDAD");
                    query.append(", FECHA_INFO ");
                    
                    query.append("  ,MAXATR");
                    
                    query.append("  ,STATUS ");
                    query.append("  ,'").append(idUsuario).append("'");
                    query.append("  ,'").append(macAddress).append("'");
                    query.append("FROM SICC_FALTANTES_ANEXO22 ");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  AND NUMERO_CLIENTE = ").append(numeroCliente);
                    System.out.println(query);
                    bd.executeUpdate(query.toString());

                    query.setLength(0);
                    query.append("UPDATE SICC_FALTANTES_ANEXO22 ");
                    query.append("SET ");
                    query.append(updateCommon.armaUpdate(listCampos, stD));
                    query.append("  ,FECHA_MODIFICACION = '").append(fechaModificacion).append("'");
                    query.append("  ,ID_USUARIO_MODIFICACION = '").append(idUsuario).append("'");
                    query.append("  ,MAC_ADDRESS_MODIFICACION = '").append(macAddress).append("'");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  AND NUMERO_CLIENTE = ").append(numeroCliente);
                    System.out.println(query);
                    bd.executeUpdate(query.toString());

                    bd.commit();
                } catch (SQLException e) {
                    bd.rollback();
                    query.setLength(0);
                    query.append("UPDATE SICC_ANEXO22_LO ");
                    query.append("SET STATUS_ALTA = 'PP' ");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  AND FOLIO_LOTE = '").append(folioLote).append("'");
                    query.append("  AND NUMERO_CONSECUTIVO = ").append(consecutivo);
                    query.append("  AND STATUS_CARGA = 'P' ");
                    System.out.println(query);
                    bd.executeUpdate(query.toString());

                    ArchivoError archivoError = new ArchivoError(numeroInstitucion, idUsuario, macAddress, folioLote);
                    archivoError.insertaError(String.valueOf(consecutivo), 989, noLayout, consecutivo);
                }
                i++;
            }
            bd.close();
        } catch (SQLException e) {
            bd.close();
            e.printStackTrace();
            throw new Exception(msjSistema.getMensaje(131));
        }
    }
}

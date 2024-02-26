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

public class SICC_ANEXO21 {

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
        expRegular.append("(\\d{0,8}+\\|)");                                      //ANTIGUEDAD_SOCIEDAD_INF_CRED
        expRegular.append("((0|1){0,1}+\\|)");                                    //CUENTA_QUITAS_CAST_REST
        expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)");                      //PORCENTAJE_PAGOS_TIEMPO_NOBANC
        expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)");                      //PORCENTAJE_PAGOS_TIEMPO_ENTCOM
        expRegular.append("(\\d{0,2}+\\|)");                                      //CUENTAS_ABIERTAS_INSTBANC
        expRegular.append("(\\d{0,21}+\\||\\d{0,21}+\\.+\\d{0,2}+\\|)");                     //MONTO_MAXIMO_CREDITO_INSTBANC
        expRegular.append("(\\d{0,4}+\\|)");                                      //MESES_ULTIMO_CRED_12MESES
        expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)");                      //PORCENTAJE_PAGOS_ATR60+_INSTBANC
        expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)");                      //PORCENTAJE_PAGOS_ATR29-_INSTBANC
        expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)");                      //PORCENTAJE_PAGOS_ATR90+_INSTBANC
        expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)");                     //PROMEDIO_DIAS_MORA_INSTBANC
        expRegular.append("(\\d{0,8}+\\|)");                                      //NUMERO_PAGOS_TIEMPO_INSTBANC
        expRegular.append("(\\d{0,21}+\\||\\d{0,21}+\\.+\\d{0,2}+\\|)");                     //APORTACIONES_INFONAVIT_ULT_BIMESTRE
        expRegular.append("(\\d{0,8}+\\|)");                                      //DIAS_ATRASO_INFONAVIT_ULT_BIMESTRE
        expRegular.append("(\\d{1,1}+\\|)");                                      //PROCESOS_ORIGINACION_ADMON
        expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)");                      //TASA_RETENCION_LABORAL
        expRegular.append("((0|1){1,1}+\\|)");                                      //ACREDITADO_SIN_ATRASO
        expRegular.append("((0?[1-9]|[12][0-9]|3[01])?(/|-)?(0?[1-9]|1[012])?(/|-)?((19|20)\\d\\d)?+\\|)");//FECHA_INFORMACION_FINANCIERA
        expRegular.append("((0?[1-9]|[12][0-9]|3[01])?(/|-)?(0?[1-9]|1[012])?(/|-)?((19|20)\\d\\d)?+\\|)");//FECHA_INFORMACION_BURO
        expRegular.append("((0|1){1,1}+\\|)");                                      //CALIFICACION_AGENCIA_CALIF
        expRegular.append("((0|1){1,1}+\\|)");                                      //EXPERIENCIA_NEGATIVA_PAGO
        expRegular.append("((0|1){1,1}+\\|)");                                      //ORG_DESC_PARTIDO_POLITICO
        expRegular.append("(\\d{0,1})");                                          //ES_GARANTE

        if (registro.toString().matches(expRegular.toString())) {
            valido = true;
        }

        return valido;
    }

    public String separaRegistros(String registro, int consecutivo, int noLayout, String folioLote, short numeroInstitucion, String idUsuario, String macAddress) throws Exception {
        InspectorDatos iData = new InspectorDatos();
        validaCampos = new ValidaCampos(numeroInstitucion, idUsuario, macAddress, folioLote);
        StringBuffer errRegistro = new StringBuffer();
        char pipe = '|';

        errRegistro.setLength(0);
        String[] cad = registro.split("\\|");
        StringTokenizer st = new StringTokenizer(registro, "|");

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

        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "ANTIGUEDAD_SOCIEDAD_INF_CRED", 8, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "CUENTA_QUITAS_CAST_REST", 1, noLayout, consecutivo, "0", (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PORCENTAJE_PAGOS_TIEMPO_NOBANC", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PORCENTAJE_PAGOS_TIEMPO_ENTCOM", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "CUENTAS_ABIERTAS_INSTBANC", 2, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "MONTO_MAXIMO_CREDITO_INSTBANC", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "MESES_ULTIMO_CRED_12MESES", 4, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PORCENTAJE_PAGOS_ATR60_INSTBANC", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PORCENTAJE_PAGOS_ATR29_INSTBANC", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PORCENTAJE_PAGOS_ATR90_INSTBANC", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PROMEDIO_DIAS_MORA_INSTBANC", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NUMERO_PAGOS_TIEMPO_INSTBANC", 8, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "APORTACIONES_INFONAVIT_ULT_BIMESTRE", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "DIAS_ATRASO_INFONAVIT_ULT_BIMESTRE", 8, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "PROCESOS_ORIGINACION_ADMON", 1, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "TASA_RETENCION_LABORAL", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "ACREDITADO_SIN_ATRASO", 1, noLayout, consecutivo, "0", (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "FECHA_INFORMACION_FINANCIERA", 10, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "FECHA_INFORMACION_BURO", 10, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "CALIFICACION_AGENCIA_CALIF", 1, noLayout, consecutivo, "0", (byte)1)).append(pipe);
        errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "EXPERIENCIA_NEGATIVA_PAGO", 1, noLayout, consecutivo, "0", (byte)1)).append(pipe);
        errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "ORG_DESC_PARTIDO_POLITICO", 1, noLayout, consecutivo, "0", (byte)1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "ES_GARANTE", 1, noLayout, consecutivo, (byte) 0)).append(pipe);

        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "POR_PAGO_TIEMPO_INSTCRED", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "CREDITO_PYME", 1, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "GARANTIA_LEY_FED", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "DOMICILIO_EXT", 1, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PI", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "PUNTAJE_TOTAL", 6, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "PUNTAJE_CUANT", 6, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "MESES_PI_100", 6, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_POR_PAG_TIEM_ENT_FIN_BAN", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_POR_PAG_TIEM_ENT_NO_BAN", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_PRES_QUITAS_CAST", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_MAX_ATR_4", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_PROM_DIAS_MORA", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "MAXATR", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "PROMEDIO_DIAS_MORA", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "FECHA_INFO", 10, noLayout, consecutivo, (byte) 1)).append(pipe);
        
        return errRegistro.toString();
    }

    public void insertaCarga(String registro, short numeroInstitucion, String nombreArchivo, String folioLote, int noLayout, String idUsuario, String macAddress) throws Exception {
        MensajesSistema msjSistema = new MensajesSistema();
        validaCampos = new ValidaCampos();
        StringTokenizer st = new StringTokenizer(registro, "|");
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
            query.append("INSERT INTO dbo.SICC_ANEXO21_LO(");
            query.append("  NUMERO_INSTITUCION");
            query.append(", NOMBRE_ARCHIVO");
            query.append(", FOLIO_LOTE");
            query.append(", NUMERO_LAYOUT");
            query.append(", NUMERO_CONSECUTIVO");
            query.append(", NUMERO_CLIENTE");
            query.append(", ANTIGUEDAD_SOCIEDAD_INF_CRED");
            query.append(", CUENTA_QUITAS_CAST_REST");
            query.append(", PORCENTAJE_PAGOS_TIEMPO_NOBANC");
            query.append(", PORCENTAJE_PAGOS_TIEMPO_ENTCOM");
            query.append(", CUENTAS_ABIERTAS_INSTBANC");
            query.append(", MONTO_MAXIMO_CREDITO_INSTBANC");
            query.append(", MESES_ULTIMO_CRED_12MESES");
            query.append(", PORCENTAJE_PAGOS_ATR60_INSTBANC");
            query.append(", PORCENTAJE_PAGOS_ATR29_INSTBANC");
            query.append(", PORCENTAJE_PAGOS_ATR90_INSTBANC");
            query.append(", PROMEDIO_DIAS_MORA_INSTBANC");
            query.append(", NUMERO_PAGOS_TIEMPO_INSTBANC");
            query.append(", APORTACIONES_INFONAVIT_ULT_BIMESTRE");
            query.append(", DIAS_ATRASO_INFONAVIT_ULT_BIMESTRE");
            query.append(", PROCESOS_ORIGINACION_ADMON");
            query.append(", TASA_RETENCION_LABORAL");
            query.append(", ACREDITADO_SIN_ATRASO");
            query.append(", FECHA_INFORMACION_FINANCIERA");
            query.append(", FECHA_INFORMACION_BURO");
            query.append(", CALIFICACION_AGENCIA_CALIF");
            query.append(", EXPERIENCIA_NEGATIVA_PAGO");
            query.append(", ORG_DESC_PARTIDO_POLITICO");
            query.append(", ES_GARANTE");
            query.append(", POR_PAGO_TIEMPO_INSTCRED");
            query.append(", CREDITO_PYME");
            query.append(", GARANTIA_LEY_FED");
            query.append(", DOMICILIO_EXT");
            query.append(", PI");
            query.append(", PUNTAJE_TOTAL");
            query.append(", PUNTAJE_CUANT");
            query.append(", MESES_PI_100");
            query.append(", P_POR_PAG_TIEM_ENT_FIN_BAN");
            query.append(", P_POR_PAG_TIEM_ENT_NO_BAN");
            query.append(", P_PRES_QUITAS_CAST");
            query.append(", P_MAX_ATR_4");
            query.append(", P_PROM_DIAS_MORA");
            query.append(", MAXATR");
            query.append(", PROMEDIO_DIAS_MORA");
            query.append(", FECHA_INFO");
            
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
            query.append(", ").append(st.nextToken());                          //NUMERO_CONSECUTIVO
            query.append(", ").append(st.nextToken());                          //NUMERO_CLIENTE
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//ANTIGUEDAD_SOCIEDAD_INF_CRED
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//CUENTA_QUITAS_CAST_REST
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//PORCENTAJE_PAGOS_TIEMPO_NOBANC
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//PORCENTAJE_PAGOS_TIEMPO_ENTCOM
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//CUENTAS_ABIERTAS_INSTBANC
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//MONTO_MAXIMO_CREDITO_INSTBANC
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//MESES_ULTIMO_CRED_12MESES
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//PORCENTAJE_PAGOS_ATR60_INSTBANC
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//PORCENTAJE_PAGOS_ATR29_INSTBANC
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//PORCENTAJE_PAGOS_ATR90_INSTBANC
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//PROMEDIO_DIAS_MORA_INSTBANC
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//NUMERO_PAGOS_TIEMPO_INSTBANC
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//APORTACIONES_INFONAVIT_ULT_BIMESTRE
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//DIAS_ATRASO_INFONAVIT_ULT_BIMESTRE
            query.append(", ").append(st.nextToken());                          //PROCESOS_ORIGINACION_ADMON
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//TASA_RETENCION_LABORAL
            query.append(", ").append(st.nextToken());                          //ACREDITADO_SIN_ATRASO
            
         /*   query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), cadena));//FECHA_INFORMACION_FINANCIERA
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), cadena));//FECHA_INFORMACION_BURO
            */
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
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//ORG_DESC_PARTIDO_POLITICO
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//ES_GARANTE
            
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//ES_GARANTE
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	POR_PAGO_TIEMPO_INSTCRED
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	CREDITO_PYME
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	GARANTIA_LEY_FED
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	DOMICILIO_EXT
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	PI

            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	PUNTAJE_TOTAL
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	PUNTAJE_CUANT
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	MESES_PI_100
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	P_POR_PAG_TIEM_ENT_FIN_BAN
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	P_POR_PAG_TIEM_ENT_NO_BAN
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	P_PRES_QUITAS_CAST

            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	P_MAX_ATR_4
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	P_PROM_DIAS_MORA
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	MAXATR
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));	//	PROMEDIO_DIAS_MORA
            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), cadena));	//	FECHA_INFO
            
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
            query.append("  AND ISC.TABLE_NAME = 'SICC_FALTANTES_ANEXO21' ");
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
            query.append(" FROM SICC_ANEXO21_LO AS STL ");
            query.append(" WHERE STL.NUMERO_CLIENTE IS NOT NULL ");
            query.append("   AND EXISTS ( ");
            query.append("     SELECT SFT.NUMERO_CLIENTE ");
            query.append("     FROM SICC_FALTANTES_ANEXO21 AS SFT ");
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
                    query.append("INSERT INTO LOG_SICC_FALTANTES_ANEXO21 (");
                    query.append("  NUMERO_INSTITUCION ");
                    query.append("  ,NUMERO_CLIENTE ");
                    query.append("  ,FECHA_MODIFICACION ");
                    query.append("  ,ANTIGUEDAD_SOCIEDAD_INF_CRED ");
                    query.append("  ,CUENTA_QUITAS_CAST_REST ");
                    query.append("  ,PORCENTAJE_PAGOS_TIEMPO_NOBANC ");
                    query.append("  ,PORCENTAJE_PAGOS_TIEMPO_ENTCOM ");
                    query.append("  ,CUENTAS_ABIERTAS_INSTBANC ");
                    query.append("  ,MONTO_MAXIMO_CREDITO_INSTBANC ");
                    query.append("  ,MESES_ULTIMO_CRED_12MESES ");
                    query.append("  ,PORCENTAJE_PAGOS_ATR60_INSTBANC ");
                    query.append("  ,PORCENTAJE_PAGOS_ATR29_INSTBANC ");
                    query.append("  ,PORCENTAJE_PAGOS_ATR90_INSTBANC ");
                    query.append("  ,PROMEDIO_DIAS_MORA_INSTBANC ");
                    query.append("  ,NUMERO_PAGOS_TIEMPO_INSTBANC ");
                    query.append("  ,APORTACIONES_INFONAVIT_ULT_BIMESTRE ");
                    query.append("  ,DIAS_ATRASO_INFONAVIT_ULT_BIMESTRE ");
                    query.append("  ,PROCESOS_ORIGINACION_ADMON ");
                    query.append("  ,TASA_RETENCION_LABORAL ");
                    query.append("  ,ACREDITADO_SIN_ATRASO ");
                    query.append("  ,FECHA_INFORMACION_FINANCIERA ");
                    query.append("  ,FECHA_INFORMACION_BURO ");
                    query.append("  ,CALIFICACION_AGENCIA_CALIF ");
                    query.append("  ,EXPERIENCIA_NEGATIVA_PAGO ");
                    query.append("  ,ORG_DESC_PARTIDO_POLITICO ");
                    query.append("  ,ES_GARANTE ");

                    query.append("  ,POR_PAGO_TIEMPO_INSTCRED");
                    query.append("  ,CREDITO_PYME");
                    query.append("  ,GARANTIA_LEY_FED");
                    query.append("  ,DOMICILIO_EXT");
                    query.append("  ,PI");
                    query.append("  ,PUNTAJE_TOTAL");
                    query.append("  ,PUNTAJE_CUANT");
                    query.append("  ,MESES_PI_100");
                    query.append("  ,P_POR_PAG_TIEM_ENT_FIN_BAN");
                    query.append("  ,P_POR_PAG_TIEM_ENT_NO_BAN");
                    query.append("  ,P_PRES_QUITAS_CAST");
                    query.append("  ,P_MAX_ATR_4");
                    query.append("  ,P_PROM_DIAS_MORA");
                    query.append("  ,MAXATR");
                    query.append("  ,PROMEDIO_DIAS_MORA");
                    query.append("  ,FECHA_INFO");
                    
                    query.append("  ,STATUS ");
                    query.append("  ,ID_USUARIO_MODIFICACION ");
                    query.append("  ,MAC_ADDRESS_MODIFICACION ");
                    query.append("  ) ");
                    query.append("SELECT NUMERO_INSTITUCION ");
                    query.append("  ,NUMERO_CLIENTE ");
                    query.append("  ,'").append(fechaModificacion).append("' ");
                    query.append("  ,ANTIGUEDAD_SOCIEDAD_INF_CRED ");
                    query.append("  ,CUENTA_QUITAS_CAST_REST ");
                    query.append("  ,PORCENTAJE_PAGOS_TIEMPO_NOBANC ");
                    query.append("  ,PORCENTAJE_PAGOS_TIEMPO_ENTCOM ");
                    query.append("  ,CUENTAS_ABIERTAS_INSTBANC ");
                    query.append("  ,MONTO_MAXIMO_CREDITO_INSTBANC ");
                    query.append("  ,MESES_ULTIMO_CRED_12MESES ");
                    query.append("  ,PORCENTAJE_PAGOS_ATR60_INSTBANC ");
                    query.append("  ,PORCENTAJE_PAGOS_ATR29_INSTBANC ");
                    query.append("  ,PORCENTAJE_PAGOS_ATR90_INSTBANC ");
                    query.append("  ,PROMEDIO_DIAS_MORA_INSTBANC ");
                    query.append("  ,NUMERO_PAGOS_TIEMPO_INSTBANC ");
                    query.append("  ,APORTACIONES_INFONAVIT_ULT_BIMESTRE ");
                    query.append("  ,DIAS_ATRASO_INFONAVIT_ULT_BIMESTRE ");
                    query.append("  ,PROCESOS_ORIGINACION_ADMON ");
                    query.append("  ,TASA_RETENCION_LABORAL ");
                    query.append("  ,ACREDITADO_SIN_ATRASO ");
                    query.append("  ,FECHA_INFORMACION_FINANCIERA ");
                    query.append("  ,FECHA_INFORMACION_BURO ");
                    query.append("  ,CALIFICACION_AGENCIA_CALIF ");
                    query.append("  ,EXPERIENCIA_NEGATIVA_PAGO ");
                    query.append("  ,ORG_DESC_PARTIDO_POLITICO ");
                    query.append("  ,ES_GARANTE ");

                    query.append(", POR_PAGO_TIEMPO_INSTCRED");
                    query.append(", CREDITO_PYME");
                    query.append(", GARANTIA_LEY_FED");
                    query.append(", DOMICILIO_EXT");
                    query.append(", PI");
                    query.append(", PUNTAJE_TOTAL");
                    query.append(", PUNTAJE_CUANT");
                    query.append(", MESES_PI_100");
                    query.append(", P_POR_PAG_TIEM_ENT_FIN_BAN");
                    query.append(", P_POR_PAG_TIEM_ENT_NO_BAN");
                    query.append(", P_PRES_QUITAS_CAST");
                    query.append(", P_MAX_ATR_4");
                    query.append(", P_PROM_DIAS_MORA");
                    query.append(", MAXATR");
                    query.append(", PROMEDIO_DIAS_MORA");
                    query.append(", FECHA_INFO");
                    
                    query.append("  ,STATUS ");
                    query.append("  ,'").append(idUsuario).append("' ");
                    query.append("  ,'").append(macAddress).append("' ");
                    query.append("FROM SICC_FALTANTES_ANEXO21 ");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  AND NUMERO_CLIENTE = ").append(numeroCliente);
                    System.out.println(query);
                    bd.executeUpdate(query.toString());

                    query.setLength(0);
                    query.append("UPDATE SICC_FALTANTES_ANEXO21 ");
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
                    query.append("UPDATE SICC_ANEXO21_LO ");
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

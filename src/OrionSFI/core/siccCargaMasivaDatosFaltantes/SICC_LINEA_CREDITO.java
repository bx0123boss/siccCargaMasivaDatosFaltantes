/*
 **********************************************************************************************************
 *    FECHA         ID.REQUERIMIENTO            DESCRIPCIÓN                            MODIFICÓ.          
 * 13/03/2015             3237            Creación del documento con            José Manuel Zúñiga Flores 
 *                                        especificaciones de la Carga de       Fernando Vázquez Galán    
 *                                        Archivo para Datos Faltantes de                                 
 *                                        Calificación de Cartera (Linea                                 
 *                                        Crédito y Código CNBV)                                          
 **********************************************************************************************************
 * 24/03/2015           3329            Carga Masiva Datos Faltantes Línea      José Manuel Zuñiga Flores 
 *                                      Crédito y Código CNBV                                             
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

public class SICC_LINEA_CREDITO {

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
        expRegular.append("(\\d{1,15}+\\|)"); // NUMERO_LINEA
        expRegular.append("([a-zA-Z0-9-_ ]{0,18}+\\|)"); //NUM_CONSULTA_SIC
        //expRegular.append("(\\d{1,5}+\\|)"); // PRODUCTO_COMERCIAL
        //expRegular.append("(\\d{1,3}+\\|)"); // TIPO_LINEA
        //expRegular.append("(\\d{1,3}+\\|)"); // POSICION
        //expRegular.append("(\\d{1,3}+\\|)"); // TIPO_OPERACION
        expRegular.append("(\\d{0,3}+\\|)"); // TIPO_ALTA
        expRegular.append("(\\d{0,3}+\\|)"); // TIPO_BAJA
        //expRegular.append("(\\d{0,3}+\\|)"); // TIPO_ALTA_MA
        //expRegular.append("(\\d{0,3}+\\|)"); // TIPO_BAJA_MA
        //expRegular.append("([a-zA-Z0-9-_ ]{1,40}+\\|)"); // NUM_EMPRESTITOS_LOCAL
        //expRegular.append("([a-zA-Z0-9-_ ]{0,40}+\\|)"); // NUM_EMPRESTITOS_SHCP
        //expRegular.append("([a-zA-Z0-9-_ ]{1,29}+\\|)"); // CODIGO_CNBV
        //expRegular.append("([a-zA-Z0-9-_ ]{1,18}+\\|)"); // FOLIO_CONSULTA_BURO
        //expRegular.append("([a-zA-Z0-9-_ ]{1,22}+\\|)"); // ES_PADRE
        //expRegular.append("((0|1){1,1}+\\|)"); // VIGENCIA_INDEFINIDA
        //expRegular.append("((0|1){1,1}+\\|)"); // CREDITO_SINDICADO
        //expRegular.append("(\\d{0,6}+\\|)"); // INSTITUTO_FONDEA
        //expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)"); // PORCENTAJE_PART_FEDERAL
        //expRegular.append("(\\d{0,21}+\\||\\d{0,21}+\\.+\\d{0,2}+\\|)"); // APOYO_BANCA_DESARROLLO
        //expRegular.append("(\\d{0,15}+\\||\\d{0,15}+\\.+\\d{0,2}+\\|)"); // SDO_QUITAS
        //expRegular.append("(\\d{0,15}+\\||\\d{0,15}+\\.+\\d{0,2}+\\|)"); // SDO_CASTIGOS
        //expRegular.append("(\\d{0,15}+\\||\\d{0,15}+\\.+\\d{0,2}+\\|)"); // SDO_CONDONACION
        //expRegular.append("(\\d{0,15}+\\||\\d{0,15}+\\.+\\d{0,2}+\\|)"); // SDO_DESCUENTOS
        //expRegular.append("(\\d{0,15}+\\||\\d{0,15}+\\.+\\d{0,2}+\\|)"); // SDO_DACION
        //expRegular.append("(\\d{0,15}+\\||\\d{0,15}+\\.+\\d{0,2}+\\|)"); // SDO_BONIFICACION
        //expRegular.append("(\\d{0,21}+\\||\\d{0,21}+\\.+\\d{0,2}+\\|)"); // COMISIONES_COBRADAS
        //expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)"); // GATOS_ORIGINACION_TASA
        //expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)"); // COMISION_DISPOSICION_TASA
        //expRegular.append("(\\d{1,21}+\\||\\d{1,21}+\\.+\\d{1,2}+\\|)"); // COMISION_DISPOSICION_MONTO
        //expRegular.append("(\\d{0,15}|\\d{0,15}+\\.+\\d{0,2})"); // COMISION_ANUALIDAD

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
                errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NUMERO_LINEA", 15, noLayout, consecutivo, (byte) 1)).append(pipe);
            	}else{
                    errRegistro.append(validaCampos.getCampoNumerico(cad[0], "NUMERO_LINEA", 15, noLayout, consecutivo, (byte) 1)).append(pipe);
            	}	
        } else {
            errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NUMERO_LINEA", 15, noLayout, consecutivo, (byte) 1)).append(pipe);
        }
        
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Numero de Consulta SIC", 18, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "PRODUCTO_COMERCIAL", 5, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TIPO_LINEA", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "POSICION", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TIPO_OPERACION", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Tipo Alta", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Tipo Baja", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TIPO_ALTA_MA", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TIPO_BAJA_MA", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Numero Emprestitos Local", 40, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "NUM_EMPRESTITOS_SHCP", 40, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Codigo CNBV", 29, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "FOLIO_CONSULTA_BURO", 18, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "ES_PADRE", 22, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "VIGENCIA_INDEFINIDA", 1, noLayout, consecutivo, "0", (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "CREDITO_SINDICADO", 1, noLayout, consecutivo, "0", (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "INSTITUTO_FONDEA", 6, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PORCENTAJE_PART_FEDERAL", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "APOYO_BANCA_DESARROLLO", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SDO_QUITAS", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SDO_CASTIGOS", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SDO_CONDONACION", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SDO_DESCUENTOS", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SDO_DACION", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SDO_BONIFICACION", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "COMISIONES_COBRADAS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "GASTOS_ORIGINACION_TASA", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "COMISION_DISPOSICION_TASA", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "COMISION_DISPOSICION_MONTO", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "COMISION_ANUALIDAD", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "OPER_DIF_TASA", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TIPO_BAJA_OTRA", 3, noLayout, consecutivo, (byte) 0)).append(pipe);

        System.out.println(errRegistro.toString());
        return errRegistro.toString();
    }

    public void insertaCarga(String registro, short numeroInstitucion, String nombreArchivo, String folioLote, int noLayout, String idUsuario, String macAddress) throws Exception {
        MensajesSistema msjSistema = new MensajesSistema();
        StringTokenizer st = new StringTokenizer(registro, "|");
        SQLProperties sqlProperties = new SQLProperties();
        validaCampos = new ValidaCampos(numeroInstitucion, idUsuario, macAddress, folioLote);
        SimpleDateFormat sdf = new SimpleDateFormat(sqlProperties.getFormatoSoloFechaOrion());
        String fechaAlta = sqlProperties.getFechaInsercionFormateada(sdf.format(new Date()));
        InspectorDatos iDat = new InspectorDatos();
        
        byte numero = 0;
        byte cadena = 1;

        try {
            bd = new JDBCConnectionPool();
            bd.setAutoCommit(false);

            query = new StringBuffer();
            query.append("INSERT INTO dbo.SICC_LINEA_CREDITO_LO(");
            query.append("  NUMERO_INSTITUCION");
            query.append("  ,NOMBRE_ARCHIVO");
            query.append("  ,FOLIO_LOTE");
            query.append("  ,NUMERO_LAYOUT");
            query.append("  ,NUMERO_CONSECUTIVO");
            query.append("  ,NUMERO_LINEA");
            query.append("  ,NUM_CONSULTA_SIC");
            query.append("  ,PRODUCTO_COMERCIAL");
            query.append("  ,TIPO_LINEA");
            query.append("  ,POSICION");
            query.append("  ,TIPO_OPERACION");
            query.append("  ,TIPO_ALTA");
            query.append("  ,TIPO_BAJA");
            query.append("  ,TIPO_ALTA_MA");
            query.append("  ,TIPO_BAJA_MA");
            query.append("  ,NUM_EMPRESTITOS_LOCAL");
            query.append("  ,NUM_EMPRESTITOS_SHCP");
            query.append("  ,CODIGO_CNBV");
            query.append("  ,FOLIO_CONSULTA_BURO");
            query.append("  ,ES_PADRE");
            query.append("  ,VIGENCIA_INDEFINIDA");
            query.append("  ,CREDITO_SINDICADO");
            query.append("  ,INSTITUTO_FONDEA");
            query.append("  ,PORCENTAJE_PART_FEDERAL");
            query.append("  ,APOYO_BANCA_DESARROLLO");
            query.append("  ,SDO_QUITAS");
            query.append("  ,SDO_CASTIGOS");
            query.append("  ,SDO_CONDONACION");
            query.append("  ,SDO_DESCUENTOS");
            query.append("  ,SDO_DACION");
            query.append("  ,SDO_BONIFICACION");
            query.append("  ,COMISIONES_COBRADAS");
            query.append("  ,GASTOS_ORIGINACION_TASA");
            query.append("  ,COMISION_DISPOSICION_TASA");
            query.append("  ,COMISION_DISPOSICION_MONTO");
            query.append("  ,COMISION_ANUALIDAD");
            query.append("  ,OPER_DIF_TASA");
            query.append("  ,TIPO_BAJA_OTRA");
            query.append("  ,STATUS_CARGA");
            query.append("  ,STATUS_ALTA");
            query.append("  ,STATUS");
            query.append("  ,FECHA_ALTA");
            query.append("  ,ID_USUARIO_ALTA");
            query.append("  ,MAC_ADDRESS_ALTA");
            query.append("  ,FECHA_MODIFICACION");
            query.append("  ,ID_USUARIO_MODIFICACION");
            query.append("  ,MAC_ADDRESS_MODIFICACION) ");
            query.append("VALUES(");
            query.append("  ").append(numeroInstitucion);
            query.append("  ,'").append(nombreArchivo).append("'");
            query.append("  ,'").append(folioLote).append("'");
            query.append("  ,").append(noLayout);
            query.append("  ,").append(st.nextToken()); // NUMERO_CONSECUTIVO
            query.append("  ,").append(st.nextToken()); // NUMERO_LINEA
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena));// NUM_CONSULTA_SIC
            //query.append("  ,").append(st.nextToken()); // NUMERO_CONSECUTIVO
            query.append(", NULL"); 
            //query.append("  ,").append(st.nextToken()); // PRODUCTO_COMERCIAL
            query.append(", NULL"); // PRODUCTO_COMERCIAL
            //query.append("  ,").append(st.nextToken()); // TIPO_LINEA
            query.append(", NULL"); // TIPO_LINEA
            //query.append("  ,").append(st.nextToken()); // TIPO_OPERACION
            query.append(", NULL"); // TIPO_OPERACION
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // TIPO_ALTA
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // TIPO_BAJA
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // TIPO_ALTA_MA
            query.append(", NULL"); // TIPO_ALTA_MA
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // TIPO_BAJA_MA
            query.append(", NULL"); // TIPO_BAJA_MA
            //String NUM_EMPRESTITOS_LOCAL = st.nextToken();
            //query.append("  ,").append(validaCampos.validaStringNull(iDat.isStringNULLExt(NUM_EMPRESTITOS_LOCAL)?"&#":NUM_EMPRESTITOS_LOCAL, cadena)); // NUM_EMPRESTITOS_LOCAL
            query.append(", NULL"); // NUM_EMPRESTITOS_LOCAL
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // NUM_EMPRESTITOS_SHCP
            query.append(", NULL"); // NUM_EMPRESTITOS_SHCP
            //String CODIGO_CNBV = st.nextToken();
            //query.append("  ,").append(validaCampos.validaStringNull(iDat.isStringNULLExt(CODIGO_CNBV)?"&#":CODIGO_CNBV, cadena)); // CODIGO_CNBV
            query.append(", NULL"); // CODIGO_CNBV
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // FOLIO_CONSULTA_BURO
            query.append(", NULL"); // FOLIO_CONSULTA_BURO
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // ES_PADRE
            query.append(", NULL"); // ES_PADRE
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // VIGENCIA_INDEFINIDA
            query.append(", NULL"); // VIGENCIA_INDEFINIDA
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // CREDITO_SINDICADO
            query.append(", NULL"); // CREDITO_SINDICADO
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // INSTITUTO_FONDEA
            query.append(", NULL"); // INSTITUTO_FONDEA
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // PORCENTAJE_GTIA_FONDO
            query.append(", NULL"); // PORCENTAJE_GTIA_FONDO
            //query.append("  ,").append(validaCampos.convierteNullCero(st.nextToken(), numero)); // APOYO_BANCA_DESARROLLO
            query.append(", NULL"); // APOYO_BANCA_DESARROLLO
            //query.append("  ,").append(validaCampos.convierteNullCero(st.nextToken(), numero)); // SDO_QUITAS
            query.append(", NULL"); // SDO_QUITAS
            //query.append("  ,").append(validaCampos.convierteNullCero(st.nextToken(), numero)); // SDO_CASTIGOS
            query.append(", NULL"); // SDO_CASTIGOS
            //query.append("  ,").append(validaCampos.convierteNullCero(st.nextToken(), numero)); // SDO_CONDONACION
            query.append(", NULL"); // SDO_CONDONACION
            //query.append("  ,").append(validaCampos.convierteNullCero(st.nextToken(), numero)); // SDO_DESCUENTOS
            query.append(", NULL"); // SDO_DESCUENTOS
            //query.append("  ,").append(validaCampos.convierteNullCero(st.nextToken(), numero)); // SDO_DACION
            query.append(", NULL"); // SDO_DACION
            //query.append("  ,").append(validaCampos.convierteNullCero(st.nextToken(), numero)); // SDO_BONIFICACION
            query.append(", NULL"); // SDO_BONIFICACION
            //query.append("  ,").append(validaCampos.convierteNullCero(st.nextToken(), numero));//COMISIONES_COBRADAS
            query.append(", NULL"); //COMISIONES_COBRADAS
            //query.append("  ,").append(validaCampos.convierteNullCero(st.nextToken(), numero));//GASTOS_ORIGINACION_TASA
            query.append(", NULL"); //GASTOS_ORIGINACION_TASA
            //query.append("  ,").append(validaCampos.convierteNullCero(st.nextToken(), numero));//COMISION_DISPOSICION_TASA
            query.append(", NULL"); //COMISION_DISPOSICION_TASA
            //query.append("  ,").append(validaCampos.convierteNullCero(st.nextToken(), numero));//COMISION_DISPOSICION_MONTO
            query.append(", NULL"); //COMISION_DISPOSICION_MONTO
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero));//COMISION_ANUALIDAD
            query.append(", NULL"); //COMISION_ANUALIDAD
            //query.append("  ,").append(validaCampos.convierteNullCero(st.nextToken(), numero));//OPER_DIF_TASA
            query.append(", NULL"); //OPER_DIF_TASA
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero));//TIPO_BAJA_OTRA
            query.append(", NULL"); //TIPO_BAJA_OTRA
            query.append("  ,'NP'");
            query.append("  ,'NP'");
            query.append("  ,1");
            query.append("  ,'").append(fechaAlta).append("'");
            query.append("  ,'").append(idUsuario).append("'");
            query.append("  ,'").append(macAddress).append("'");
            query.append("  ,NULL");
            query.append("  ,NULL");
            query.append("  ,NULL)");
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
        
        byte indAlta = 0;
        byte indBaja = 0;

        try {
            bd = new JDBCConnectionPool();

            query = new StringBuffer("");
            query.append("SELECT UPPER(ISC.DATA_TYPE) AS TIPO_DATO ");
            query.append("  ,ISC.COLUMN_NAME AS CAMPO ");
            query.append("FROM SICC_CAMPOS_LAYOUT AS SCL ");
            query.append("INNER JOIN INFORMATION_SCHEMA.COLUMNS ISC ON ISC.COLUMN_NAME = SCL.NOMBRE_CAMPO ");
            query.append("  AND ISC.TABLE_NAME = 'SICC_FALTANTES_LINEA_CREDITO' ");
            query.append("  AND SCL.IND_ACTUALIZA = 1 ");
            query.append("  AND SCL.NUMERO_INSTITUCION = ").append(numeroInstitucion);
            query.append("  AND SCL.NUMERO_LAYOUT = ").append(noLayout);
            System.out.println(query);
            resultadoCampos = bd.executeQuery(query.toString());
            listCampos = sqlProperties.getColumnValue(resultadoCampos, listCampos);
            String campos;
            int i = 0;

            query.setLength(0);
            query.append("SELECT STL.NUMERO_LINEA ");
            query.append("  ,STL.NUMERO_CONSECUTIVO ");
            while (i < listCampos.size()) {
                campos = listCampos.get(i).toString();
                campos = campos.substring(campos.indexOf("&#") + 2, campos.length());
                if (!campos.equals("MONTO_PAGO_EFECTIVO_COMISION") && !campos.equals("SDO_QUEBRANTO")) {
                    query.append("  ,STL.").append(campos);
                }
                i++;
            }
            query.append(" FROM SICC_LINEA_CREDITO_LO AS STL ");
            query.append(" WHERE STL.NUMERO_LINEA IS NOT NULL ");
            query.append("   AND EXISTS ( ");
            query.append("     SELECT SFT.NUMERO_LINEA ");
            query.append("     FROM SICC_FALTANTES_LINEA_CREDITO AS SFT ");
            query.append("     WHERE STL.NUMERO_INSTITUCION = SFT.NUMERO_INSTITUCION ");
            query.append("       AND STL.NUMERO_LINEA = SFT.NUMERO_LINEA ");
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
                    String numeroLinea = stD.nextToken();
                    consecutivo = Integer.parseInt(stD.nextToken());
                    bd.setAutoCommit(false);
                    query.setLength(0);
                    query.append("INSERT INTO LOG_SICC_FALTANTES_LINEA_CREDITO (");
                    query.append("  NUMERO_INSTITUCION ");
                    query.append("  ,NUMERO_LINEA ");
                    query.append("  ,FECHA_MODIFICACION ");
                    query.append("  ,NUM_CONSULTA_SIC");
                    query.append("  ,PRODUCTO_COMERCIAL ");
                    query.append("  ,TIPO_LINEA ");
                    query.append("  ,POSICION ");
                    query.append("  ,TIPO_OPERACION ");
                    query.append("  ,TIPO_ALTA ");
                    query.append("  ,TIPO_BAJA ");
                    query.append("  ,TIPO_ALTA_MA ");
                    query.append("  ,TIPO_BAJA_MA ");
                    query.append("  ,NUM_EMPRESTITOS_LOCAL ");
                    query.append("  ,NUM_EMPRESTITOS_SHCP ");
                    query.append("  ,CODIGO_CNBV ");
                    query.append("  ,FOLIO_CONSULTA_BURO ");
                    query.append("  ,ES_PADRE ");
                    query.append("  ,VIGENCIA_INDEFINIDA ");
                    query.append("  ,CREDITO_SINDICADO ");
                    query.append("  ,INSTITUTO_FONDEA ");
                    query.append("  ,PORCENTAJE_PART_FEDERAL ");
                    query.append("  ,APOYO_BANCA_DESARROLLO ");
                    query.append("  ,SDO_QUITAS ");
                    query.append("  ,SDO_CASTIGOS ");
                    query.append("  ,SDO_CONDONACION ");
                    query.append("  ,SDO_QUEBRANTO ");
                    query.append("  ,SDO_DESCUENTOS ");
                    query.append("  ,SDO_DACION ");
                    query.append("  ,SDO_BONIFICACION ");
                    query.append("  ,COMISIONES_COBRADAS ");
                    query.append("  ,GASTOS_ORIGINACION_TASA ");
                    query.append("  ,COMISION_DISPOSICION_TASA ");
                    query.append("  ,COMISION_DISPOSICION_MONTO ");
                    query.append("  ,COMISION_ANUALIDAD ");
                    query.append("  ,MONTO_PAGO_EFECTIVO_COMISION ");
                    
                    query.append("  ,OPER_DIF_TASA ");
                    query.append("  ,TIPO_BAJA_OTRA ");
                    
                    query.append("  ,STATUS ");
                    query.append("  ,ID_USUARIO_MODIFICACION ");
                    query.append("  ,MAC_ADDRESS_MODIFICACION ");
                    query.append("  ) ");
                    query.append("SELECT NUMERO_INSTITUCION ");
                    query.append("  ,NUMERO_LINEA ");
                    query.append("  ,'").append(fechaModificacion).append("'");
                    query.append("  ,NUM_CONSULTA_SIC");
                    query.append("  ,PRODUCTO_COMERCIAL ");
                    query.append("  ,TIPO_LINEA ");
                    query.append("  ,POSICION ");
                    query.append("  ,TIPO_OPERACION ");
                    query.append("  ,TIPO_ALTA ");
                    query.append("  ,TIPO_BAJA ");
                    query.append("  ,TIPO_ALTA_MA ");
                    query.append("  ,TIPO_BAJA_MA ");
                    query.append("  ,NUM_EMPRESTITOS_LOCAL ");
                    query.append("  ,NUM_EMPRESTITOS_SHCP ");
                    query.append("  ,CODIGO_CNBV ");
                    query.append("  ,FOLIO_CONSULTA_BURO ");
                    query.append("  ,ES_PADRE ");
                    query.append("  ,VIGENCIA_INDEFINIDA ");
                    query.append("  ,CREDITO_SINDICADO ");
                    query.append("  ,INSTITUTO_FONDEA ");
                    query.append("  ,PORCENTAJE_PART_FEDERAL ");
                    query.append("  ,APOYO_BANCA_DESARROLLO ");
                    query.append("  ,SDO_QUITAS ");
                    query.append("  ,SDO_CASTIGOS ");
                    query.append("  ,SDO_CONDONACION ");
                    query.append("  ,SDO_QUEBRANTO ");
                    query.append("  ,SDO_DESCUENTOS ");
                    query.append("  ,SDO_DACION ");
                    query.append("  ,SDO_BONIFICACION ");
                    query.append("  ,COMISIONES_COBRADAS ");
                    query.append("  ,GASTOS_ORIGINACION_TASA ");
                    query.append("  ,COMISION_DISPOSICION_TASA ");
                    query.append("  ,COMISION_DISPOSICION_MONTO ");
                    query.append("  ,COMISION_ANUALIDAD ");
                    query.append("  ,MONTO_PAGO_EFECTIVO_COMISION ");
                    query.append("  ,OPER_DIF_TASA ");
                    query.append("  ,TIPO_BAJA_OTRA ");
                    query.append("  ,STATUS ");
                    query.append("  ,'").append(idUsuario).append("'");
                    query.append("  ,'").append(macAddress).append("'");
                    query.append("FROM SICC_FALTANTES_LINEA_CREDITO ");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  AND NUMERO_LINEA = ").append(numeroLinea);
                    System.out.println(query);
                    bd.executeUpdate(query.toString());

                    indAlta = getvalidaAltaBaja(numeroInstitucion, Long.parseLong(numeroLinea),(short) 1, bd);
                    indBaja = getvalidaAltaBaja(numeroInstitucion, Long.parseLong(numeroLinea),(short) 2, bd);
                    
                    query.setLength(0);
                    query.append("UPDATE SICC_FALTANTES_LINEA_CREDITO ");
                    query.append("SET ");
                    query.append(updateCommon.armaUpdateCredito(listCampos, stD, indAlta, indBaja));
                    query.append("  ,FECHA_MODIFICACION = '").append(fechaModificacion).append("'");
                    query.append("  ,ID_USUARIO_MODIFICACION = '").append(idUsuario).append("'");
                    query.append("  ,MAC_ADDRESS_MODIFICACION = '").append(macAddress).append("'");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  AND NUMERO_LINEA = ").append(numeroLinea);
                    System.out.println(query);
                    bd.executeUpdate(query.toString());

                    indAlta = 0;
                    indBaja = 0;
                    
                    bd.commit();
                } catch (SQLException e) {
                    bd.rollback();
                    e.printStackTrace();
                    query.setLength(0);
                    query.append("UPDATE SICC_LINEA_CREDITO_LO ");
                    query.append("SET STATUS_ALTA = 'PP' ");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  AND FOLIO_LOTE = '").append(folioLote).append("' ");
                    query.append("  AND NUMERO_CONSECUTIVO = ").append(consecutivo);
                    query.append("  AND STATUS_CARGA = 'P'");
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
    
    public byte getvalidaAltaBaja(short numeroInstitucion, long numeroLinea, short indicador, JDBCConnectionPool bd) throws Exception {
    	List<String> listDatos = new ArrayList<String>();
    	byte actualiza = 0;
    	try {
    		StringBuffer query = new StringBuffer();
    		
    		query.append(" "); 
    		query.append("SELECT NUMERO_LINEA "); 
    		query.append(" ,FECHA_APERTURA "); 
    		query.append(" ,FECHA_CIERRE "); 
//    		query.append(" ,CASE "); 
//    		query.append("  WHEN FECHA_APERTURA IS NOT NULL "); 
//    		query.append("   THEN CASE "); 
//    		query.append("     WHEN FECHA_ACTUAL >= FECHA_APERTURA "); 
//    		query.append("      AND ( "); 
//    		query.append("       ( "); 
//    		query.append("        DATEPART(MONTH, FECHA_APERTURA) = DATEPART(MONTH, FECHA_ACTUAL) "); 
//    		query.append("        AND DATEPART(YEAR, FECHA_APERTURA) = DATEPART(YEAR, FECHA_ACTUAL) "); 
//    		query.append("        ) "); 
//    		query.append("       OR ( "); 
//    		query.append("        DATEPART(MONTH, FECHA_APERTURA) = DATEPART(MONTH, FECHA_ULTIMO_FINMES) "); 
//    		query.append("        AND DATEPART(YEAR, FECHA_APERTURA) = DATEPART(YEAR, FECHA_ULTIMO_FINMES) "); 
//    		query.append("        AND B.FECHA_ACTUAL <= C.FECHA_DEPURACION "); 
//    		query.append("        ) "); 
//    		query.append("       ) "); 
//    		query.append("      THEN 1 "); 
//    		query.append("     ELSE 0 "); 
//    		query.append("     END "); 
//    		query.append("  ELSE 0 "); 
//    		query.append("  END AS IND_ALTA "); 
    		query.append("  ,1 AS IND_ALTA "); 
//    		query.append(" ,CASE "); 
//    		query.append("  WHEN FECHA_CIERRE IS NOT NULL "); 
//    		query.append("   THEN CASE "); 
//    		query.append("     WHEN FECHA_ACTUAL >= FECHA_CIERRE "); 
//    		query.append("      AND ( "); 
//    		query.append("       ( "); 
//    		query.append("        DATEPART(MONTH, FECHA_CIERRE) = DATEPART(MONTH, FECHA_ACTUAL) "); 
//    		query.append("        AND DATEPART(YEAR, FECHA_CIERRE) = DATEPART(YEAR, FECHA_ACTUAL) "); 
//    		query.append("        ) "); 
//    		query.append("       OR ( "); 
//    		query.append("        DATEPART(MONTH, FECHA_CIERRE) = DATEPART(MONTH, FECHA_ULTIMO_FINMES) "); 
//    		query.append("        AND DATEPART(YEAR, FECHA_CIERRE) = DATEPART(YEAR, FECHA_ULTIMO_FINMES) "); 
//    		query.append("        AND B.FECHA_ACTUAL <= C.FECHA_DEPURACION "); 
//    		query.append("        ) "); 
//    		query.append("       ) "); 
//    		query.append("      THEN 1 "); 
//    		query.append("     ELSE 0 "); 
//    		query.append("     END "); 
//    		query.append("  ELSE 0 "); 
//    		query.append("  END AS IND_BAJA "); 
    		query.append("  ,1 AS IND_BAJA "); 
    		query.append("FROM CRE_LINEAS A "); 
    		query.append("INNER JOIN GRAL_FECHAS_SISTEMA B ON A.NUMERO_INSTITUCION = B.NUMERO_INSTITUCION "); 
    		query.append(" AND B.CLAVE_SISTEMA = 'CREDITO' ");
    		query.append("INNER JOIN SICC_PARAMETROS C ON A.NUMERO_INSTITUCION = C.NUMERO_INSTITUCION "); 
    		query.append("WHERE A.NUMERO_INSTITUCION = ").append(numeroInstitucion);
    		query.append(" AND NUMERO_LINEA = ").append(numeroLinea);
    		
    		System.out.println(query);
			ResultSet resultado = bd.executeQuery(query.toString());

			if (resultado.next()) {
				if(indicador == 1){
					actualiza = resultado.getByte("IND_ALTA");
				}else if(indicador == 2){
					actualiza = resultado.getByte("IND_BAJA");
				}
			}

    	} catch (SQLException se) {
    		se.printStackTrace();
    		MensajesSistema mensajeSistema = new MensajesSistema();
    		throw new SQLException(mensajeSistema.getMensaje(131));
    	}
    	return actualiza;
    }
    
//    public static void main(String arg[]) throws Exception {
//        SICC_LINEA_CREDITO obj = new SICC_LINEA_CREDITO();
//        String registro = "351| | | | | | | | |LOCAL_02122016|SHCP_02122016|CNBV_02122016|BURO_02122016|1|0|1| |10000|10000|10000|10000|10000|10000|10000|10000|10000|1000|1000|10000|10000|";
//        obj.separaRegistros(registro, 1, dosDecimales, "12", (short)1, "USRCONFIG", "00:00:00:00:00:00");
//    }
}

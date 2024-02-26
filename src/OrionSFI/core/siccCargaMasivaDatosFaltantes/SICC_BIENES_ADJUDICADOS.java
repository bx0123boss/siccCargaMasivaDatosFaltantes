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

public class SICC_BIENES_ADJUDICADOS {

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
        // expRegular.append("\\d{1,5}+\\|"); // NUMERO_CONSECUTIVO
        expRegular.append("\\d{1,22}+\\|"); // NUMERO_CLIENTE
        expRegular.append("([a-zA-Z0-9-_ ]{0,50}+\\|)"); //NUMERO_CONTRATO
        expRegular.append("\\d{0,1}+\\|"); //ESTATUS_BIEN
        expRegular.append("([a-zA-Z0-9-_ ]{0,12}+\\|)"); //CLASIFICACION_CONTABLE
        expRegular.append("\\d{0,1}+\\|"); //ORIGEN_BIEN
        expRegular.append("([a-zA-Z0-9-_ ]{0,100}+\\|)"); //DESCRIPCION_BIEN
        expRegular.append("([a-zA-Z0-9-_ ]{0,20}+\\|)"); //ID_BIEN_ADJUDICADO
        expRegular.append("\\d{0,21}+[\\.]?+\\d{0,2}+\\|"); //MONTO_BIEN_ADJUDICADO
        expRegular.append("([a-zA-Z0-9-_ ]{0,20}+\\|)"); //NUMERO_ACTA_RES
        expRegular.append("^([0-2][0-9]|3[0-1])(\\/|-)(0[1-9]|1[0-2])\\2(\\d{4})$+\\|"); //FECHA_ACTA_RES
        expRegular.append("([a-zA-Z0-9-_ ]{0,20}+\\|)"); //NUMERO_ESCRITURA
        expRegular.append("^([0-2][0-9]|3[0-1])(\\/|-)(0[1-9]|1[0-2])\\2(\\d{4})$+\\|"); //FECHA_ESCRITURA
        expRegular.append("\\d{0,21}+[\\.]?+\\d{0,2}+\\|"); //MONTO_ESCRITURA
        expRegular.append("^([0-2][0-9]|3[0-1])(\\/|-)(0[1-9]|1[0-2])\\2(\\d{4})$+\\|"); //FECHA_AUTORIZACION_DACION
        expRegular.append("([a-zA-Z0-9-_ ]{0,100}+\\|)"); //ORGANISMO_AUT_DA
        expRegular.append("\\d{0,21}+[\\.]?+\\d{0,2}+\\|"); //MONTO_AUTORIZADO_DAC
        expRegular.append("^([0-2][0-9]|3[0-1])(\\/|-)(0[1-9]|1[0-2])\\2(\\d{4})$+\\|"); //FECHA_CONTRATO_DAC
        expRegular.append("\\d{0,21}+[\\.]?+\\d{0,2}+\\|"); //MONTO_CONTRATO_DAC
        expRegular.append("\\d{0,21}+[\\.]?+\\d{0,2}+\\|"); //CAPITAL_PAGADO
        expRegular.append("\\d{0,21}+[\\.]?+\\d{0,2}+\\|"); //INT_ORDINARIOS_PAG
        expRegular.append("\\d{0,21}+[\\.]?+\\d{0,2}+\\|"); //INT_MORA_PAG
        expRegular.append("\\d{0,21}+[\\.]?+\\d{0,2}+\\|"); //OTROS_ADEUDOS_PAG
        expRegular.append("\\d{0,21}+[\\.]?+\\d{0,2}+\\|"); //IMPACTO_RESULTADOS
        expRegular.append("\\d{0,4}+\\|"); //MESES_ANTIGUEDAD_REGISTRO
        expRegular.append("\\d{0,21}+[\\.]?+\\d{0,2}+\\|"); //MONTO_ESTIMACION
        expRegular.append("([a-zA-Z0-9-_ ]{0,1}+\\|)"); //OFERTA_COMPRA_BIEN
        expRegular.append("^([0-2][0-9]|3[0-1])(\\/|-)(0[1-9]|1[0-2])\\2(\\d{4})$+\\|"); //FECHA_EST_COMPRAVENTA
        expRegular.append("([a-zA-Z0-9-_ ]{0,100}+\\|)"); //ORGANISMO_AUT_OPERAC
        expRegular.append("^([0-2][0-9]|3[0-1])(\\/|-)(0[1-9]|1[0-2])\\2(\\d{4})$+\\|"); //FECHA_AUTORIZACION_OPER
        expRegular.append("\\d{0,21}+[\\.]?+\\d{0,2}+\\|");//MONTO_AUTORIZADO_OPER
        expRegular.append("^([0-2][0-9]|3[0-1])(\\/|-)(0[1-9]|1[0-2])\\2(\\d{4})$+\\|"); //FECHA_OPERACION
        expRegular.append("\\d{0,21}+[\\.]?+\\d{0,2}+\\|"); //MONTO_OPERACION_CONTRATO
        expRegular.append("\\d{0,21}+[\\.]?+\\d{0,2}+\\|"); //MONTO_RESULTADOS
        expRegular.append("([a-zA-Z0-9-_ ]{0,1}+\\|)"); //RESULTADO_OPER

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
        	if(cad[0].equalsIgnoreCase("NULL"))
        		errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NUMERO_CLIENTE", 15, noLayout, consecutivo, (byte) 1)).append(pipe);
        	else
        		errRegistro.append(validaCampos.getCampoNumerico(cad[0], "NUMERO_CLIENTE", 15, noLayout, consecutivo, (byte) 1)).append(pipe);
        } else {
            errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NUMERO_CLIENTE", 15, noLayout, consecutivo, (byte) 1)).append(pipe);
        }

        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Numero de Contrato", 50, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Estatus del Bien", 1, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Clasificacion Contable", 12, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Origen del Bien", 1, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Descripcion del Bien", 100, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "ID del bien adjudicado", 20, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Monto del bien adjudicado", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Numero Acta Resolucion", 20, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "Fecha Acta Resolucion", 10, noLayout, consecutivo, (byte) 1, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Numero de Escritura", 20, noLayout, consecutivo, (byte) 0)).append(pipe); 
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "Fecha de Escritura", 10, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Monto de la Escritura", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "Fecha de Autorizacion Dacion", 10, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Organismo que Autorizo", 100, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Monto Autorizado", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "Fecha de Contrato", 10, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Monto de Contrato", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Captal Pagado", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Interes Ordinario Pagado", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Interes Moratorio Pagado", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Otros Adeudos Pagados", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Impacto de Resultados", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Meses de Antiguedad de Registro", 4, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Monto de Estimacion", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Oferta de Compra del Bien", 1, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "Fecha Estimada de Compra Venta", 10, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Organismo que Autorizo la Operacion", 100, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "Fecha de Autorizacion de Operacion", 10, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Monto Autorizado de la Operacion", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "Fecha de Operacion", 10, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Monto de Operacion del Contrato", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Monto Aplicado a Resultados", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Resultado de la Operacion", 1, noLayout, consecutivo, (byte) 1)).append(pipe);
        
        return errRegistro.toString();

    }

    public void insertaCarga(String registro, short numeroInstitucion, String nombreArchivo, String folioLote, int noLayout, String idUsuario, String macAddress) throws Exception {
        MensajesSistema msjSistema = new MensajesSistema();
        StringTokenizer st = new StringTokenizer(registro, "|");
        validaCampos = new ValidaCampos(numeroInstitucion, idUsuario, macAddress, folioLote);
        byte cadena = 1;
        byte numero = 0;

        SQLProperties sqlProperties = new SQLProperties();
        SimpleDateFormat sdf = new SimpleDateFormat(sqlProperties.getFormatoSoloFechaOrion());
        String fechaAlta = sqlProperties.getFechaInsercionFormateada(sdf.format(new Date()));
        try {

            bd = new JDBCConnectionPool();

            query = new StringBuffer();
            query.append("INSERT INTO SICC_BIENES_ADJUDICADOS_LO(");
            query.append("  NUMERO_INSTITUCION");
            query.append("  ,NOMBRE_ARCHIVO");
            query.append("  ,FOLIO_LOTE");
            query.append("  ,NUMERO_LAYOUT");
            query.append("  ,NUMERO_CONSECUTIVO");
            query.append("  ,NUMERO_CLIENTE");
            query.append("  ,NUMERO_CONTRATO");
            query.append("  ,ESTATUS_BIEN");
            query.append("  ,CLASIFICACION_CONTABLE");
            query.append("  ,ORIGEN_BIEN");
            query.append("  ,DESCRIPCION_BIEN");
            query.append("  ,ID_BIEN_ADJUDICADO");
            query.append("  ,MONTO_BIEN_ADJUDICADO");
            query.append("  ,NUMERO_ACTA_RES");
            query.append("  ,FECHA_ACTA_RES");
            query.append("  ,NUMERO_ESCRITURA");
            query.append("  ,FECHA_ESCRITURA");
            query.append("  ,MONTO_ESCRITURA");
            query.append("  ,FECHA_AUTORIZACION_DACION");
            query.append("  ,ORGANISMO_AUT_DA");
            query.append("  ,MONTO_AUTORIZADO_DAC");
            query.append("  ,FECHA_CONTRATO_DAC");
            query.append("  ,MONTO_CONTRATO_DAC");
            query.append("  ,CAPITAL_PAGADO");
            query.append("  ,INT_ORDINARIOS_PAG");
            query.append("  ,INT_MORA_PAG");
            query.append("  ,OTROS_ADEUDOS_PAG");
            query.append("  ,IMPACTO_RESULTADOS");
            query.append("  ,MESES_ANTIGUEDAD_REGISTRO");
            query.append("  ,MONTO_ESTIMACION");
            query.append("  ,OFERTA_COMPRA_BIEN");
            query.append("  ,FECHA_EST_COMPRAVENTA");
            query.append("  ,ORGANISMO_AUT_OPERAC");
            query.append("  ,FECHA_AUTORIZACION_OPER");
            query.append("  ,MONTO_AUTORIZADO_OPER");
            query.append("  ,FECHA_OPERACION");
            query.append("  ,MONTO_OPERACION_CONTRATO");
            query.append("  ,MONTO_RESULTADOS");
            query.append("  ,RESULTADO_OPER ");
            query.append("  ,STATUS_CARGA");
            query.append("  ,STATUS_ALTA ");
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
            query.append("  ,").append(st.nextToken()); // NUMERO_CLIENTE
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //NUMERO_CONTRATO
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //ESTATUS_BIEN
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //CLASIFICACION_CONTABLE
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //ORIGEN_BIEN
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //DESCRIPCION_BIEN
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //ID_BIEN_ADJUDICADO
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //MONTO_BIEN_ADJUDICADO
            query.append("	,").append(validaCampos.convierteNullCero(st.nextToken(), cadena)); //NUMERO_ACTA_RES
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //FECHA_ACTA_RES
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //NUMERO_ESCRITURA
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //FECHA_ESCRITURA
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //MONTO_ESCRITURA
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //FECHA_AUTORIZACION_DACION
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //ORGANISMO_AUT_DA
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //MONTO_AUTORIZADO_DAC
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //FECHA_CONTRATO_DAC
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //MONTO_CONTRATO_DAC
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //CAPITAL_PAGADO
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //INT_ORDINARIOS_PAG
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //INT_MORA_PAG
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //OTROS_ADEUDOS_PAG
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //IMPACTO_RESULTADOS
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //MESES_ANTIGUEDAD_REGISTRO
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //MONTO_ESTIMACION
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //OFERTA_COMPRA_BIEN
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //FECHA_EST_COMPRAVENTA
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //ORGANISMO_AUT_OPERAC
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //FECHA_AUTORIZACION_OPER
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //MONTO_AUTORIZADO_OPER
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //FECHA_OPERACION
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //MONTO_OPERACION_CONTRATO
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //MONTO_RESULTADOS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //RESULTADO_OPER
            query.append("  ,'NP'"); // STATUS_CARGA
            query.append("  ,'NP'");
            query.append("  ,1");
            query.append("  ,'").append(fechaAlta).append("'");
            query.append("  ,'").append(idUsuario).append("'");
            query.append("  ,'").append(macAddress).append("'");
            query.append("  ,NULL");
            query.append("  ,NULL");
            query.append("  ,NULL)");
            System.out.println(query);
            bd.executeUpdate(query.toString());

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
            query.append("  AND ISC.TABLE_NAME = 'SICC_FALTANTES_BIENESADJUDICADOS' ");
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
            query.append(" FROM SICC_BIENES_ADJUDICADOS_LO AS STL ");
            query.append(" WHERE STL.NUMERO_CLIENTE IS NOT NULL ");
            query.append("   AND EXISTS ( ");
            query.append("     SELECT SFT.NUMERO_CLIENTE ");
            query.append("     FROM SICC_FALTANTES_BIENESADJUDICADOS AS SFT ");
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
                    query.append("INSERT INTO LOG_SICC_FALTANTES_BIENESADJUDICADOS (");
                    query.append("  NUMERO_INSTITUCION ");
                    query.append("  ,NUMERO_CLIENTE ");
                    query.append("  ,FECHA_MODIFICACION ");
                    query.append("  ,NUMERO_CONTRATO");
                    query.append("  ,ESTATUS_BIEN");
                    query.append("  ,CLASIFICACION_CONTABLE");
                    query.append("  ,ORIGEN_BIEN");
                    query.append("  ,DESCRIPCION_BIEN");
                    query.append("  ,ID_BIEN_ADJUDICADO");
                    query.append("  ,MONTO_BIEN_ADJUDICADO");
                    query.append("  ,NUMERO_ACTA_RES");
                    query.append("  ,FECHA_ACTA_RES");
                    query.append("  ,NUMERO_ESCRITURA");
                    query.append("  ,FECHA_ESCRITURA");
                    query.append("  ,MONTO_ESCRITURA");
                    query.append("  ,FECHA_AUTORIZACION_DACION");
                    query.append("  ,ORGANISMO_AUT_DA");
                    query.append("  ,MONTO_AUTORIZADO_DAC");
                    query.append("  ,FECHA_CONTRATO_DAC");
                    query.append("  ,MONTO_CONTRATO_DAC");
                    query.append("  ,CAPITAL_PAGADO");
                    query.append("  ,INT_ORDINARIOS_PAG");
                    query.append("  ,INT_MORA_PAG");
                    query.append("  ,OTROS_ADEUDOS_PAG");
                    query.append("  ,IMPACTO_RESULTADOS");
                    query.append("  ,MESES_ANTIGUEDAD_REGISTRO");
                    query.append("  ,MONTO_ESTIMACION");
                    query.append("  ,OFERTA_COMPRA_BIEN");
                    query.append("  ,FECHA_EST_COMPRAVENTA");
                    query.append("  ,ORGANISMO_AUT_OPERAC");
                    query.append("  ,FECHA_AUTORIZACION_OPER");
                    query.append("  ,MONTO_AUTORIZADO_OPER");
                    query.append("  ,FECHA_OPERACION");
                    query.append("  ,MONTO_OPERACION_CONTRATO");
                    query.append("  ,MONTO_RESULTADOS");
                    query.append("  ,RESULTADO_OPER ");
                    query.append("  ,STATUS ");
                    query.append("  ,ID_USUARIO_MODIFICACION ");
                    query.append("  ,MAC_ADDRESS_MODIFICACION ");
                    query.append("  ) ");
                    query.append("SELECT NUMERO_INSTITUCION ");
                    query.append("  ,NUMERO_CLIENTE ");
                    query.append("  ,'").append(fechaModificacion).append("'");
                    query.append("  ,NUMERO_CONTRATO");
                    query.append("  ,ESTATUS_BIEN");
                    query.append("  ,CLASIFICACION_CONTABLE");
                    query.append("  ,ORIGEN_BIEN");
                    query.append("  ,DESCRIPCION_BIEN");
                    query.append("  ,ID_BIEN_ADJUDICADO");
                    query.append("  ,MONTO_BIEN_ADJUDICADO");
                    query.append("  ,NUMERO_ACTA_RES");
                    query.append("  ,FECHA_ACTA_RES");
                    query.append("  ,NUMERO_ESCRITURA");
                    query.append("  ,FECHA_ESCRITURA");
                    query.append("  ,MONTO_ESCRITURA");
                    query.append("  ,FECHA_AUTORIZACION_DACION");
                    query.append("  ,ORGANISMO_AUT_DA");
                    query.append("  ,MONTO_AUTORIZADO_DAC");
                    query.append("  ,FECHA_CONTRATO_DAC");
                    query.append("  ,MONTO_CONTRATO_DAC");
                    query.append("  ,CAPITAL_PAGADO");
                    query.append("  ,INT_ORDINARIOS_PAG");
                    query.append("  ,INT_MORA_PAG");
                    query.append("  ,OTROS_ADEUDOS_PAG");
                    query.append("  ,IMPACTO_RESULTADOS");
                    query.append("  ,MESES_ANTIGUEDAD_REGISTRO");
                    query.append("  ,MONTO_ESTIMACION");
                    query.append("  ,OFERTA_COMPRA_BIEN");
                    query.append("  ,FECHA_EST_COMPRAVENTA");
                    query.append("  ,ORGANISMO_AUT_OPERAC");
                    query.append("  ,FECHA_AUTORIZACION_OPER");
                    query.append("  ,MONTO_AUTORIZADO_OPER");
                    query.append("  ,FECHA_OPERACION");
                    query.append("  ,MONTO_OPERACION_CONTRATO");
                    query.append("  ,MONTO_RESULTADOS");
                    query.append("  ,RESULTADO_OPER ");
                    query.append("  ,STATUS ");
                    query.append("  ,'").append(idUsuario).append("'");
                    query.append("  ,'").append(macAddress).append("'");
                    query.append("FROM SICC_FALTANTES_BIENESADJUDICADOS ");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  AND NUMERO_CLIENTE = ").append(numeroCliente);
                    System.out.println(query);
                    bd.executeUpdate(query.toString());

                    query.setLength(0);
                    query.append("UPDATE SICC_FALTANTES_BIENESADJUDICADOS ");
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
                    query.append("UPDATE SICC_BIENES_ADJUDICADOS_LO ");
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
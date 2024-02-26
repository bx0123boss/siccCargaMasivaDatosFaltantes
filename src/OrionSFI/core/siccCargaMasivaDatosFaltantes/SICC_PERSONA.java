/*
 *********************************************************************************************************************
 *    FECHA         ID.REQUERIMIENTO            DESCRIPCIÓN                            MODIFICÓ.          *
 * 09/02/2015             3226            Creación del documento con            José Manuel Zúñiga Flores *
 *                                        especificaciones de la Carga de       Fernando Vázquez Galán    *
 *                                        Archivo para Datos Faltantes de                                 *
 *                                        Calificación de Cartera                                         *
 *                                                                                                        *
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

public class SICC_PERSONA {

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
        //expRegular.append("\\d{0,1}+\\|"); // RELACIONADO_ACREDITADO
        expRegular.append("\\d{0,3}+\\|");  // TIPO_ACRED_REL
        expRegular.append("\\d{1,21}+\\|"); // INGRESOS_MENSUALES
        expRegular.append("\\d{0,2}+\\|"); //PERIODICIDAD_INGRESOS
        expRegular.append("\\d{1,8}+\\|"); //NUM_EMPLEADOS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); //INGRESOS_ANUALES
        //expRegular.append("\\d{0,1}+\\|");  // COMPROBANTE_INGRESOS
        //expRegular.append("\\d{1,15}+[\\.]?+\\d{0,2}+\\|"); // INGRESOS_BRUTOS
        //expRegular.append("\\d{0,3}+\\|"); // ENTIDAD_GOBIERNO
        //expRegular.append("\\d{0,1}+\\|"); // SECTOR_LABORAL
        //expRegular.append("(0|1){1,1}+\\|"); // CUMPLE_CRITERIO_CONT
        //expRegular.append("\\d{0,1}+\\|"); // ES_FONDO
        //expRegular.append("[a-zA-Z0-9-_ ]{0,20}+\\|"); // ID_BURO_CREDITO
        //expRegular.append("[a-zA-Z0-9-_ ]{0,18}+\\|"); // FOLIO_CONSULTA_BURO
        //expRegular.append("[a-zA-Z0-9-_ ]{0,20}+\\|"); // LEI
        //expRegular.append("\\d{0,1}+\\|"); // HITSIC
        //expRegular.append("(0|1){1,1}+\\|"); // ENTIDAD_FINANCIERA
        //expRegular.append("\\d{1,15}+[\\.]?+\\d{0,2}+\\|"); // VENTAS_NETAS_TOTALES_ANUALES
        //expRegular.append("\\d{1,3}+\\|"); // SECTOR_ECONOMICO_CNBV
        //expRegular.append("\\d{1,3}+\\|"); // PI100
        //expRegular.append("\\d{1,8}+\\|"); // CALIFICACION
        //expRegular.append("\\d{1,3}+\\|"); // GRUPO_RIESGO
        //expRegular.append("\\d{1,3}+\\|"); // RELACIONADO_ACREDITADO_MA
        //expRegular.append("\\d{1,1}+\\|"); // PERSONALIDAD_JURIDICA_MA
        //expRegular.append("\\d{0,6}+\\|"); // AGENCIA_LARGO_PLAZO
        //expRegular.append("[a-zA-Z0-9-_ ]{0,8}+\\|"); // CALIFICACION_LARGO_PLAZO
        //expRegular.append("\\d{0,6}+\\|"); // AGENCIA_CORTO_PLAZO
        //expRegular.append("[a-zA-Z0-9-_ ]{0,8}+\\|"); // CALIFICACION_CORTO_PLAZO
        //expRegular.append("[a-zA-Z0-9-_ ]{1,22}"); // CODIGO_CLIENTE_IPAB

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

        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Tipo Acreditado Relacionado", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Ingresos Mensuales", 21, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Periodicidad de Ingresos", 2, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Numero de Empleados", 8, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Ingresos Anuales", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "RELACIONADO_ACREDITADO", 1, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "COMPROBANTE_INGRESOS", 1, noLayout, consecutivo, (byte) 1,(byte)1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "INGRESOS_BRUTOS", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
//        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "ENTIDAD_GOBIERNO", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "SECTOR_LABORAL", 1, noLayout, consecutivo, (byte) 1,(byte)1)).append(pipe);
//        errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "CUMPLE_CRITERIO_CONT", 1, noLayout, consecutivo, "0", (byte)0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "ES_FONDO", 1, noLayout, consecutivo, (byte) 0)).append(pipe);
//        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "ID_BURO_CREDITO", 20, noLayout, consecutivo, (byte) 0)).append(pipe);
//        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "FOLIO_CONSULTA_BURO", 18, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "LEI", 20, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "HITSIC", 1, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "ENTIDAD_FINANCIERA", 1, noLayout, consecutivo, "0", (byte)1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "VENTAS_NETAS_TOTALES_ANUALES", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "SECTOR_ECONOMICO_CNBV", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "PI100", 3, noLayout, consecutivo, (byte) 1,(byte)1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "CALIFICACION", 8, noLayout, consecutivo, (byte) 1,(byte)1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "GRUPO_RIESGO", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
//        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "RELACIONADO_ACREDITADO_MA", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
//        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "PERSONALIDAD_JURIDICA_MA", 1, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Agencia a Largo Plazo", 6, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoCalificacion(st.nextToken(), "Calificacion a Largo Plazo", 8, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Agencia a Corto Plazo", 6, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoCalificacion(st.nextToken(), "Calificacion a Corto Plazo", 8, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Codigo Cliente IPAB", 22, noLayout, consecutivo, validaCampos.getRequeridoCampo(numeroInstitucion, "1", "24"))).append(pipe); 
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TIPO_CARTERA", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "RC_TABLAADEUDO", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "RC_GRADORIESGO", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "RC_ESCALACALIF", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "RC_AGENCIA_CALIF", 6, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PI_NIF_C16", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PI_MI", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PI_PERSONA_CNBV", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TAMANO_ACREDITADO", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "FECHA_INFO", 10, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Tipo de Aval", 1, noLayout, consecutivo, (byte) 1)).append(pipe);
        
        
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
            query.append("INSERT INTO SICC_PERSONA_LO(");
            query.append("  NUMERO_INSTITUCION");
            query.append("  ,NOMBRE_ARCHIVO");
            query.append("  ,FOLIO_LOTE");
            query.append("  ,NUMERO_LAYOUT");
            query.append("  ,NUMERO_CONSECUTIVO");
            query.append("  ,NUMERO_CLIENTE");
            query.append("	,TIPO_ACRED_REL ");
            query.append("	,INGRESOS_MENSUALES ");
            query.append("	,PERIODICIDAD_INGRESOS ");
            query.append("	,NUM_EMPLEADOS ");
            query.append("	,INGRESOS_ANUALES ");
            query.append("  ,RELACIONADO_ACREDITADO");
            query.append("  ,COMPROBANTE_INGRESOS");
            query.append("  ,INGRESOS_BRUTOS");
            query.append("  ,ENTIDAD_GOBIERNO");
            query.append("  ,SECTOR_LABORAL");
            query.append("  ,CUMPLE_CRITERIO_CONT");
            query.append("  ,ES_FONDO");
            query.append("  ,ID_BURO_CREDITO");
            query.append("  ,FOLIO_CONSULTA_BURO");
            query.append("  ,LEI");
            query.append("  ,HITSIC");
            query.append("  ,ENTIDAD_FINANCIERA");
            query.append("  ,VENTAS_NETAS_TOTALES_ANUALES");
            query.append("  ,SECTOR_ECONOMICO_CNBV");
            query.append("  ,PI100");
            query.append("  ,CALIFICACION");
            query.append("  ,GRUPO_RIESGO");
            query.append("  ,RELACIONADO_ACREDITADO_MA");
            query.append("  ,PERSONALIDAD_JURIDICA_MA");
            query.append("  ,AGENCIA_LARGO_PLAZO");
            query.append("  ,CALIFICACION_LARGO_PLAZO");
            query.append("  ,AGENCIA_CORTO_PLAZO");
            query.append("  ,CALIFICACION_CORTO_PLAZO");
            query.append("  ,CODIGO_CLIENTE_IPAB");
            query.append("  ,TIPO_CARTERA");
            query.append("  ,RC_TABLAADEUDO");
            query.append("  ,RC_GRADORIESGO");
            query.append("  ,RC_ESCALACALIF");
            query.append("  ,RC_AGENCIA_CALIF");
            query.append("  ,PI_NIF_C16");
            query.append("  ,PI_MI");
            query.append("  ,PI_PERSONA_CNBV");
            //query.append("  ,TAMANO_ACREDITADO");
            query.append("  ,FECHA_INFO");
            query.append("  ,TIPO_CTE_GAR");
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
            query.append("  ,").append(st.nextToken()); // NUMERO_CLIENTE
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //TIPO_ACRED_REL
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //INGRESOS_MENSUALES
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //PERIODICIDAD_INGRESOS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //NUM_EMPLEADOS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //INGRESOS_ANUALES
            query.append("  ,").append(/*validaCampos.validaStringNull(st.nextToken(), numero)*/"NULL"); // RELACIONADO_ACREDITADO
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // COMPROBANTE_INGRESOS
            query.append("  ,NULL"); // COMPROBANTE_INGRESOS
            //query.append("  ,").append(st.nextToken()); // INGRESOS_BRUTOS
            query.append("  ,NULL"); // INGRESOS_BRUTOS
//            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // ENTIDAD_GOBIERNO
            query.append("  ,NULL"); // ENTIDAD_GOBIERNO
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // SECTOR_LABORAL
            query.append("  ,NULL"); // SECTOR_LABORAL
//            query.append("  ,").append(st.nextToken()); // CUMPLE_CRITERIO_CONT
            query.append("  ,NULL"); // CUMPLE_CRITERIO_CONT
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // ES_FONDO
            query.append("  ,NULL"); // ES_FONDO
//            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // ID_BURO_CREDITO
            query.append("  ,NULL"); // ID_BURO_CREDITO
//            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // FOLIO_CONSULTA_BURO
            query.append("  ,NULL"); // FOLIO_CONSULTA_BURO
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // LEI
            query.append("  ,NULL"); //LEI
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // HITSIC
            query.append("  ,NULL"); //HITSIC
            //query.append("  ,").append(st.nextToken()); // ENTIDAD_FINANCIERA
            query.append("  ,NULL"); //ENTIDAD_FINANCIERA
            //query.append("  ,").append(st.nextToken()); // VENTAS_NETAS_TOTALES_ANUALES
            query.append("  ,NULL"); //VENTAS_NETAS_TOTALES_ANUALES
            //query.append("  ,").append(st.nextToken()); // SECTOR_ECONOMICO_CNBV
            query.append("  ,NULL"); //SECTOR_ECONOMICO_CNBV
            //query.append("  ,").append(st.nextToken()); // PI100
            query.append("  ,NULL"); //PI100
            //query.append("  ,").append(st.nextToken()); // CALIFICACION
            query.append("  ,NULL"); // CALIFICACION
            //query.append("  ,").append(st.nextToken()); // GRUPO_RIESGO
            query.append("  ,NULL"); // GRUPO_RIESGO
//            query.append("  ,").append(st.nextToken()); // RELACIONADO_ACREDITADO_MA
            query.append("  ,NULL"); // RELACIONADO_ACREDITADO_MA
//            query.append("  ,").append(st.nextToken()); // PERSONALIDAD_JURIDICA_MA
            query.append("  ,NULL"); // PERSONALIDAD_JURIDICA_MA
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // AGENCIA_LARGO_PLAZO
            query.append("  ,NULL"); // AGENCIA_LARGO_PLAZO
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // CALIFICACION_LARGO_PLAZO
            query.append("  ,NULL"); // CALIFICACION_LARGO_PLAZO
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // AGENCIA_CORTO_PLAZO
            query.append("  ,NULL"); // AGENCIA_CORTO_PLAZO
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // CALIFICACION_CORTO_PLAZO
            query.append("  ,NULL"); // CALIFICACION_CORTO_PLAZO
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // CODIGO_CLIENTE_IPAB
            query.append("  ,NULL"); // CODIGO_CLIENTE_IPAB
            //query.append("  ,").append(st.nextToken()); // TIPO_CARTERA
            query.append("  ,NULL"); // TIPO_CARTERA
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // RC_TABLAADEUDO
            query.append("  ,NULL"); // RC_TABLAADEUDO
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // RC_GRADORIESGO
            query.append("  ,NULL"); // RC_GRADORIESGO
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // RC_ESCALACALIF
            query.append("  ,NULL"); // RC_ESCALACALIF
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // RC_AGENCIA_CALIF
            query.append("  ,NULL"); // RC_AGENCIA_CALIF
            //query.append("  ,").append(st.nextToken()); // PI_NIF_C16
            query.append("  ,NULL"); // PI_NIF_C16
            //query.append("  ,").append(st.nextToken()); // PI_MI
            query.append("  ,NULL"); // PI_MI
            //query.append("  ,").append(st.nextToken()); // PI_PERSONA_CNBV
            query.append("  ,NULL"); // PI_PERSONA_CNBV
            //query.append("  ,").append(st.nextToken()); // TAMANO_ACREDITADO
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // FECHA_INFO
            query.append("  ,NULL"); // FECHA_INFO
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append("  ,NULL");//TIPO_CTE_GAR
            
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
            query.append("  AND ISC.TABLE_NAME = 'SICC_FALTANTES_PERSONA' ");
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
            query.append(" FROM SICC_PERSONA_LO AS STL ");
            query.append(" WHERE STL.NUMERO_CLIENTE IS NOT NULL ");
            query.append("   AND EXISTS ( ");
            query.append("     SELECT SFT.NUMERO_CLIENTE ");
            query.append("     FROM SICC_FALTANTES_PERSONA AS SFT ");
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
                    query.append("INSERT INTO LOG_SICC_FALTANTES_PERSONA (");
                    query.append("  NUMERO_INSTITUCION ");
                    query.append("  ,NUMERO_CLIENTE ");
                    query.append("  ,FECHA_MODIFICACION ");
                    query.append("	,TIPO_ACRED_REL ");
                    query.append("	,INGRESOS_MENSUALES ");
                    query.append("	,PERIODICIDAD_INGRESOS ");
                    query.append("	,NUM_EMPLEADOS ");
                    query.append("	,INGRESOS_ANUALES ");
                    query.append("  ,RELACIONADO_ACREDITADO ");
                    query.append("  ,COMPROBANTE_INGRESOS ");
                    query.append("  ,INGRESOS_BRUTOS ");
                    query.append("  ,ENTIDAD_GOBIERNO ");
                    query.append("  ,SECTOR_LABORAL ");
                    query.append("  ,CUMPLE_CRITERIO_CONT ");
                    query.append("  ,ES_FONDO ");
                    query.append("  ,ID_BURO_CREDITO ");
                    query.append("  ,FOLIO_CONSULTA_BURO ");
                    query.append("  ,LEI ");
                    query.append("  ,HITSIC ");
                    query.append("  ,ENTIDAD_FINANCIERA ");
                    query.append("  ,VENTAS_NETAS_TOTALES_ANUALES ");
                    query.append("  ,SECTOR_ECONOMICO_CNBV ");
                    query.append("  ,PI100 ");
                    query.append("  ,CALIFICACION ");
                    query.append("  ,GRUPO_RIESGO ");
                    query.append("  ,RELACIONADO_ACREDITADO_MA ");
                    query.append("  ,PERSONALIDAD_JURIDICA_MA ");
                    query.append("  ,AGENCIA_LARGO_PLAZO ");
                    query.append("  ,CALIFICACION_LARGO_PLAZO ");
                    query.append("  ,AGENCIA_CORTO_PLAZO ");
                    query.append("  ,CALIFICACION_CORTO_PLAZO ");
                    query.append("  ,CODIGO_CLIENTE_IPAB ");
                    
                    query.append("  ,TIPO_CARTERA");
            		query.append("  ,RC_TABLAADEUDO");
    				query.append("  ,RC_GRADORIESGO");
					query.append("  ,RC_ESCALACALIF");
					query.append("  ,RC_AGENCIA_CALIF");
					query.append("  ,PI_NIF_C16");
					query.append("  ,PI_MI");
					query.append("  ,PI_PERSONA_CNBV");
					//query.append("  ,TAMANO_ACREDITADO");
					query.append("  ,FECHA_INFO");
					query.append("  ,TIPO_CTE_GAR");
                    
                    query.append("  ,STATUS ");
                    query.append("  ,ID_USUARIO_MODIFICACION ");
                    query.append("  ,MAC_ADDRESS_MODIFICACION ");
                    query.append("  ) ");
                    query.append("SELECT NUMERO_INSTITUCION ");
                    query.append("  ,NUMERO_CLIENTE ");
                    query.append("  ,'").append(fechaModificacion).append("'");
                    query.append("	,TIPO_ACRED_REL ");
                    query.append("	,INGRESOS_MENSUALES ");
                    query.append("	,PERIODICIDAD_INGRESOS ");
                    query.append("	,NUM_EMPLEADOS ");
                    query.append("	,INGRESOS_ANUALES ");
                    query.append("  ,RELACIONADO_ACREDITADO ");
                    query.append("  ,COMPROBANTE_INGRESOS ");
                    query.append("  ,INGRESOS_BRUTOS ");
                    query.append("  ,ENTIDAD_GOBIERNO ");
                    query.append("  ,SECTOR_LABORAL ");
                    query.append("  ,CUMPLE_CRITERIO_CONT ");
                    query.append("  ,ES_FONDO ");
                    query.append("  ,ID_BURO_CREDITO ");
                    query.append("  ,FOLIO_CONSULTA_BURO ");
                    query.append("  ,LEI ");
                    query.append("  ,HITSIC ");
                    query.append("  ,ENTIDAD_FINANCIERA ");
                    query.append("  ,VENTAS_NETAS_TOTALES_ANUALES ");
                    query.append("  ,SECTOR_ECONOMICO_CNBV ");
                    query.append("  ,PI100 ");
                    query.append("  ,CALIFICACION ");
                    query.append("  ,GRUPO_RIESGO ");
                    query.append("  ,RELACIONADO_ACREDITADO_MA ");
                    query.append("  ,PERSONALIDAD_JURIDICA_MA ");
                    query.append("  ,AGENCIA_LARGO_PLAZO ");
                    query.append("  ,CALIFICACION_LARGO_PLAZO ");
                    query.append("  ,AGENCIA_CORTO_PLAZO ");
                    query.append("  ,CALIFICACION_CORTO_PLAZO ");
                    query.append("  ,CODIGO_CLIENTE_IPAB ");
                    
                    query.append("  ,TIPO_CARTERA");
            		query.append("  ,RC_TABLAADEUDO");
    				query.append("  ,RC_GRADORIESGO");
					query.append("  ,RC_ESCALACALIF");
					query.append("  ,RC_AGENCIA_CALIF");
					query.append("  ,PI_NIF_C16");
					query.append("  ,PI_MI");
					query.append("  ,PI_PERSONA_CNBV");
					//query.append("  ,TAMANO_ACREDITADO");
					query.append("  ,FECHA_INFO");
					query.append("  ,TIPO_CTE_GAR");

                    query.append("  ,STATUS ");
                    query.append("  ,'").append(idUsuario).append("'");
                    query.append("  ,'").append(macAddress).append("'");
                    query.append("FROM SICC_FALTANTES_PERSONA ");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  AND NUMERO_CLIENTE = ").append(numeroCliente);
                    System.out.println(query);
                    bd.executeUpdate(query.toString());

                    query.setLength(0);
                    query.append("UPDATE SICC_FALTANTES_PERSONA ");
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
                    query.append("UPDATE SICC_PERSONA_LO ");
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
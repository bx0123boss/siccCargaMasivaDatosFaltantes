/*
 ************************************************************************************************************
 *    FECHA       ID.REQUERIMIENTO              DESCRIPCIÓN                                         MODIFICÓ.           
 * 15/01/2016           4920             Se cambian reglas de extracción de layouts               Uriel Caame
 *                                       Persona, Crédito, Anexo 20, 21, 22 y Aval
 *                                       Se eliminan campos de los layouts Persona, Crédito y Aval
 *                                       Se agregan campos al layout de Línea Crédito
 *                                       Cambio a nombre de estructura de Datos Faltantes 
 *                                       para layout de Linea Credito se crea layout 
 *                                       de faltantes para Garantías  se cambia tipo y 
 *                                       longitid de datos de layout de Info Financiera
 ************************************************************************************************************
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

public class SICC_GARANTIAS {

    private JDBCConnectionPool bd = null;
    private StringBuffer query = null;
    private ValidaCampos validaCampos = null;
    private static int seisDecimales = 6;
    private static int dosDecimales = 2;

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
        expRegular.append("(\\d{1,9}+\\|)");                      //NUMERO_GARANTIA
        expRegular.append("([a-zA-Z0-9-_ +]{0,2}+\\|)"); //ENDOSO
        expRegular.append("([a-zA-Z0-9-_ +]{0,2}+\\|)"); //TIPO_SEGURO
        expRegular.append("(\\d{0,21}+\\|)"); //MONTO_SEGURO
        expRegular.append("(\\d{0,12}+\\|)"); //NUMERO_CONTRATO_GAR_LIQ
        expRegular.append("([a-zA-Z0-9-_ +]{0,2}+\\|)"); //CARTA_MANDATO_GAR_LIQ
        expRegular.append("([a-zA-Z0-9-_ +]{0,2}+\\|)"); //TIPO_COBERTURA
        expRegular.append("([a-zA-Z0-9-_ +]{0,4}+\\|)"); //CLAS_PROVEEDOR_PROT
        expRegular.append("([a-zA-Z0-9-_ +]{0,250}+\\|)"); //NOMBRE_OBLIGADO
        expRegular.append("(\\d{0,21}+\\||\\d{0,21}+\\.+\\d{0,2}+\\|)"); //MONTO_OBLIGADO
        expRegular.append("([a-zA-Z0-9-_ +]{0,250}+\\|)"); //NOMBRE_AVAL
        expRegular.append("(\\d{0,21}+\\||\\d{0,21}+\\.+\\d{0,2}+\\|)"); //MONTO_AVAL
        //expRegular.append("(\\d{1,2}+\\|)");                      //TIPO_GARANTIA
        //expRegular.append("(\\d{1,2}+\\|)");                      //TIPO_GARANTIA_MA
        //expRegular.append("(\\d{0,6}+\\|)");                      //BANCO_GARANTIA
        //expRegular.append("(\\d{0,15}+\\||\\d{0,15}+\\.+\\d{0,2}+\\|)");     //VALOR_GARANTIA_PROYECTADO
        //expRegular.append("([a-zA-Z0-9-_ ]{0,20}+\\|)");          //REG_GARANTIA_MOBILIARIA
        //expRegular.append("((0|1){1,1}+\\|)");                    //IND_REAL_FINANCIERA
        //expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)");      //HC
        //expRegular.append("((0?[1-9]|[12][0-9]|3[01])?(/|-)?(0?[1-9]|1[012])?(/|-)?((19|20)\\d\\d)?+\\|)"); //VENCIMIENTO_RESTANTE
        //expRegular.append("(\\d{0,1}+\\|)");                      //GRADO_RIESGO
        //expRegular.append("(\\d{0,1}+\\|)");                      //AGENCIA_CALIFICADORA
        //expRegular.append("([a-zA-Z0-9-_ +]{0,5}+\\|)");           //CALIFICACION
        //expRegular.append("(\\d{0,1}+\\|)");                      //EMISOR
        //expRegular.append("(\\d{0,1}+\\|)");                      //ES_ACCION
        //expRegular.append("(\\d{0,1}+\\|)");                      //ES_IPC
        //expRegular.append("(\\d{0,1}+\\|)");                      //ESCALA
        //expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)");      //PI_GARANTE
        //expRegular.append("([a-zA-Z0-9-_ ]{0,40}+\\|)");          //REGISTRO_PUBLICO_PROPIEDAD
        //expRegular.append("([a-zA-Z0-9-_ ]{0,20})");              //LEI

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
        	if(cad[0].equalsIgnoreCase("NULL"))
        		errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NUMERO_GARANTIA", 9, noLayout, consecutivo, (byte) 1)).append(pipe);
        	else
        		errRegistro.append(validaCampos.getCampoNumerico(cad[0], "NUMERO_GARANTIA", 9, noLayout, consecutivo, (byte) 1)).append(pipe);
        } else {
            errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NUMERO_GARANTIA", 9, noLayout, consecutivo, (byte) 1)).append(pipe);
        }
        
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Endoso", 2, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "Fecha de Ultima Actualizacion de Garantia" , 10, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "Fecha Registro RUG" , 10, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Tipo de Seguro", 2, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "Fecha Alta Seguro" , 10, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Monto de Seguro", 21, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Numero Contrato Garantia Liquida", 12, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Carta Mandato Garantia Liquida", 2, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Tipo de Cobertura", 2, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Clasificacion de Proveedor de Proteccion", 4, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Nombre Obligado Solidario", 250, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Monto Valor Obligado", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Nombre del Aval", 250, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Monto del Aval", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TIPO_GARANTIA", 2, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TIPO_GARANTIA_MA", 2, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "BANCO_GARANTIA", 6, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "VALOR_GARANTIA_PROYECTADO", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "REG_GARANTIA_MOBILIARIA", 20, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "IND_REAL_FINANCIERA", 1, noLayout, consecutivo, "0", (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "HC", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "VENCIMIENTO_RESTANTE", 10, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "GRADO_RIESGO", 1, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "AGENCIA_CALIFICADORA", 1, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "CALIFICACION", 5, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "EMISOR", 1, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "ES_ACCION", 1, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "ES_IPC", 1, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "ESCALA", 1, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PI_GARANTE", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "REGISTRO_PUBLICO_PROPIEDAD", 40, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "LEI", 20, noLayout, consecutivo, (byte) 0)).append(pipe);
        
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "PRELACION_GAR", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "FOLIO_REF_FACTURA", 150, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "RUORL", 40, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "ROEEFM", 40, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "FECHA_ULT_AVALUO", 10, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "FECHA_INSCRIPCION_RPPC", 10, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "FECHA_INSCRIPCION_RUG", 10, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "FECHA_INSCRIPCION_RUORL", 10, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "FECHA_INSCRIPCION_ROEEFM", 10, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "LOCALIDAD_GARANTIA_INM", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "HFX", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "FECHA_INFO" ,10, noLayout, consecutivo, (byte) 1)).append(pipe);


        return errRegistro.toString();
    }

    public void insertaCarga(String registro, short numeroInstitucion, String nombreArchivo, String folioLote, int noLayout, String idUsuario, String macAddress) throws Exception {
        MensajesSistema msjSistema = new MensajesSistema();
        SQLProperties sqlProperties = new SQLProperties();
        validaCampos = new ValidaCampos(numeroInstitucion, idUsuario, macAddress, folioLote);
        SimpleDateFormat sdf = new SimpleDateFormat(sqlProperties.getFormatoSoloFechaOrion());
        String fechaAlta = sqlProperties.getFechaInsercionFormateada(sdf.format(new Date()));

        StringTokenizer st = new StringTokenizer(registro, "|");
        byte isNumeric = 0;
        byte isString = 1;

        try {
            bd = new JDBCConnectionPool();
            bd.setAutoCommit(false);

            query = new StringBuffer();
            query.append("INSERT INTO SICC_GARANTIA_LO(");
            query.append("  NUMERO_INSTITUCION");
            query.append("  ,NOMBRE_ARCHIVO");
            query.append("  ,FOLIO_LOTE");
            query.append("  ,NUMERO_LAYOUT");
            query.append("  ,NUMERO_CONSECUTIVO");
            query.append("  ,NUMERO_GARANTIA");
            query.append("  ,ENDOSO");
            query.append("  ,FECHA_ULT_ACT_GAR");
            query.append("  ,FECHA_REGISTRO_RUG");
            query.append("  ,TIPO_SEGURO");
            query.append("  ,FECHA_ALTA_SEGURO");
            query.append("  ,MONTO_SEGURO");
            query.append("  ,NUMERO_CONTRATO_GAR_LIQ");
            query.append("  ,CARTA_MANDATO_GAR_LIQ");
            query.append("  ,TIPO_COBERTURA");
            query.append("  ,CLAS_PROVEEDOR_PROT");
            query.append("  ,NOMBRE_OBLIGADO");
            query.append("  ,MONTO_OBLIGADO");
            query.append("  ,NOMBRE_AVAL");
            query.append("  ,MONTO_AVAL");
            query.append("  ,TIPO_GARANTIA");
            query.append("  ,TIPO_GARANTIA_MA");
            query.append("  ,BANCO_GARANTIA");
            query.append("  ,VALOR_GARANTIA_PROYECTADO");
            query.append("  ,REG_GARANTIA_MOBILIARIA");
            query.append("  ,IND_REAL_FINANCIERA");
            query.append("  ,HC");
            query.append("  ,VENCIMIENTO_RESTANTE");
            query.append("  ,GRADO_RIESGO");
            query.append("  ,AGENCIA_CALIFICADORA");
            query.append("  ,CALIFICACION");
            query.append("  ,EMISOR");
            query.append("  ,ES_ACCION");
            query.append("  ,ES_IPC");
            query.append("  ,ESCALA");
            query.append("  ,PI_GARANTE");
            query.append("  ,REGISTRO_PUBLICO_PROPIEDAD");
            query.append("  ,LEI");
            query.append("  ,STATUS_CARGA");
            query.append("  ,STATUS_ALTA");
            
            query.append(", PRELACION_GAR");
            query.append(", FOLIO_REF_FACTURA");
            query.append(", RUORL");
            query.append(", ROEEFM");
            query.append(", FECHA_ULT_AVALUO");
            query.append(", FECHA_INSCRIPCION_RPPC");
            query.append(", FECHA_INSCRIPCION_RUG");
            query.append(", FECHA_INSCRIPCION_RUORL");
            query.append(", FECHA_INSCRIPCION_ROEEFM");
            query.append(", LOCALIDAD_GARANTIA_INM");
            //query.append(", HFX");
            query.append(", FECHA_INFO ");
                    
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
            query.append("  ,").append(st.nextToken());                                           //NUMERO_CONSECUTIVO          
            query.append("  ,").append(st.nextToken());                                           //NUMERO_GARANTIA
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //ENDOSO
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //FECHA_ULT_ACT_GAR
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //FECHA_REGISTRO_RUG
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //TIPO_SEGURO
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //FECHA_ALTA_SEGURO
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //MONTO_SEGURO
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //NUMERO_CONTRATO_GAR_LIQ
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //CARTA_MANDATO_GAR_LIQ
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //TIPO_COBERTURA
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //CLAS_PROVEEDOR_PROT
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //NOMBRE_OBLIGADO
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //MONTO_OBLIGADO
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //NOMBRE_AVAL
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //MONTO_AVAL
            //query.append("  ,").append(st.nextToken());                                           //TIPO_GARANTIA
            query.append(", NULL");//TIPO_GARANTIA
            //query.append("  ,").append(st.nextToken());                                           //TIPO_GARANTIA_MA
            query.append(", NULL");//TIPO_GARANTIA_MA
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //BANCO_GARANTIA
            query.append(", NULL");//BANCO_GARANTIA
            //query.append("  ,").append(validaCampos.convierteNullCero(st.nextToken(), isNumeric)); //VALOR_GARANTIA_PROYECTADO
            query.append(", NULL");//VALOR_GARANTIA_PROYECTADO
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //REG_GARANTIA_MOBILIARIA
            query.append(", NULL");//REG_GARANTIA_MOBILIARIA
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //IND_REAL_FINANCIERA
            query.append(", NULL");//IND_REAL_FINANCIERA
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //HC
            query.append(", NULL");//HC
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //VENCIMIENTO_RESTANTE
            query.append(", NULL");//VENCIMIENTO_RESTANTE
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString)); //GRADO_RIESGO
            query.append(", NULL");//GRADO_RIESGO
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //AGENCIA_CALIFICADORA
            query.append(", NULL");//AGENCIA_CALIFICADORA
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //CALIFICACION
            query.append(", NULL");//CALIFICACION
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //EMISOR
            query.append(", NULL");//EMISOR
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //ES_ACCION
            query.append(", NULL");//ES_ACCION
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //ES_IPC
            query.append(", NULL");//ES_IPC
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //ESCALA
            query.append(", NULL");//ESCALA
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //PI_GARANTE
            query.append(", NULL");//PI_GARANTE
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //REGISTRO_PUBLICO_PROPIEDAD
            query.append(", NULL");//REGISTRO_PUBLICO_PROPIEDAD
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //LEI
            query.append(", NULL");//LEI
            query.append("  ,'NP'");
            query.append("  ,'NP'");
            
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //PRELACION_GAR
            query.append(", NULL");//PRELACION_GAR
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //FOLIO_REF_FACTURA
            query.append(", NULL");//FOLIO_REF_FACTURA
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //RUORL
            query.append(", NULL");//RUORL
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //ROEEFM
            query.append(", NULL");//ROEEFM
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //FECHA_ULT_AVALUO
            query.append(", NULL");//FECHA_ULT_AVALUO
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //FECHA_INSCRIPCION_RPPC
            query.append(", NULL");//FECHA_INSCRIPCION_RPPC
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //FECHA_INSCRIPCION_RUG
            query.append(", NULL");//FECHA_INSCRIPCION_RUG
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //FECHA_INSCRIPCION_RUORL
            query.append(", NULL");//FECHA_INSCRIPCION_RUORL
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //FECHA_INSCRIPCION_ROEEFM
            query.append(", NULL");//FECHA_INSCRIPCION_ROEEFM
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //LOCALIDAD_GARANTIA_INM
            query.append(", NULL");//LOCALIDAD_GARANTIA_INM
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //HFX
            //query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //FECHA_INFO
            query.append(", NULL");//FECHA_INFO
            
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

        try {
            bd = new JDBCConnectionPool();

            query = new StringBuffer("");
            query.append("SELECT UPPER(ISC.DATA_TYPE) AS TIPO_DATO ");
            query.append("  ,ISC.COLUMN_NAME AS CAMPO ");
            query.append("FROM SICC_CAMPOS_LAYOUT AS SCL ");
            query.append("INNER JOIN INFORMATION_SCHEMA.COLUMNS ISC ON ISC.COLUMN_NAME = SCL.NOMBRE_CAMPO ");
            query.append("  AND ISC.TABLE_NAME = 'SICC_FALTANTES_GARANTIAS' ");
            query.append("  AND SCL.IND_ACTUALIZA = 1 ");
            query.append("  AND SCL.NUMERO_INSTITUCION = ").append(numeroInstitucion);
            query.append("  AND SCL.NUMERO_LAYOUT = ").append(noLayout);
            System.out.println(query);
            resultadoCampos = bd.executeQuery(query.toString());
            listCampos = sqlProperties.getColumnValue(resultadoCampos, listCampos);
            String campos;
            int i = 0;

            query.setLength(0);
            query.append("SELECT STL.NUMERO_GARANTIA ");
            query.append("  ,STL.NUMERO_CONSECUTIVO ");
            while (i < listCampos.size()) {
                campos = listCampos.get(i).toString();
                campos = campos.substring(campos.indexOf("&#") + 2, campos.length());
                if (!campos.equals("IND_FACTOR_HC") && !campos.equals("IND_CALCULO_HC")) {
                    query.append("  ,STL.").append(campos);
                }
                i++;
            }
            query.append(" FROM SICC_GARANTIA_LO AS STL ");
            query.append(" WHERE STL.NUMERO_GARANTIA IS NOT NULL ");
            query.append("   AND EXISTS ( ");
            query.append("     SELECT SFT.NUMERO_GARANTIA ");
            query.append("     FROM SICC_FALTANTES_GARANTIAS AS SFT ");
            query.append("     WHERE STL.NUMERO_INSTITUCION = SFT.NUMERO_INSTITUCION ");
            query.append("       AND STL.NUMERO_GARANTIA = SFT.NUMERO_GARANTIA ");
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
            //String fechaModificacion = sqlProperties.getFechaInsercionFormateada(formato.format(fechaActual));

            while (i < listDatos.size()) {
                try {
                    StringTokenizer stD = new StringTokenizer(listDatos.get(i).toString(), "&#");
                    String numeroGarantia = stD.nextToken();
                    consecutivo = Integer.parseInt(stD.nextToken());
                    bd.setAutoCommit(false);
                    query.setLength(0);
                    query.append("INSERT INTO LOG_SICC_FALTANTES_GARANTIAS (");
                    query.append("  NUMERO_INSTITUCION ");
                    query.append("  ,NUMERO_GARANTIA ");
                    query.append("  ,FECHA_MODIFICACION ");
                    query.append("  ,ENDOSO");
                    query.append("  ,FECHA_ULT_ACT_GAR");
                    query.append("  ,FECHA_REGISTRO_RUG");
                    query.append("  ,TIPO_SEGURO");
                    query.append("  ,FECHA_ALTA_SEGURO");
                    query.append("  ,MONTO_SEGURO");
                    query.append("  ,NUMERO_CONTRATO_GAR_LIQ");
                    query.append("  ,CARTA_MANDATO_GAR_LIQ");
                    query.append("  ,TIPO_COBERTURA");
                    query.append("  ,CLAS_PROVEEDOR_PROT");
                    query.append("  ,NOMBRE_OBLIGADO");
                    query.append("  ,MONTO_OBLIGADO");
                    query.append("  ,NOMBRE_AVAL");
                    query.append("  ,MONTO_AVAL");
                    query.append("  ,TIPO_GARANTIA ");
                    query.append("  ,TIPO_GARANTIA_MA ");
                    query.append("  ,BANCO_GARANTIA ");
                    query.append("  ,VALOR_GARANTIA_PROYECTADO ");
                    query.append("  ,REG_GARANTIA_MOBILIARIA ");
                    query.append("  ,IND_REAL_FINANCIERA ");
                    query.append("  ,IND_FACTOR_HC ");
                    query.append("  ,HC ");
                    query.append("  ,IND_CALCULO_HC ");
                    query.append("  ,VENCIMIENTO_RESTANTE ");
                    query.append("  ,GRADO_RIESGO ");
                    query.append("  ,AGENCIA_CALIFICADORA ");
                    query.append("  ,CALIFICACION ");
                    query.append("  ,EMISOR ");
                    query.append("  ,ES_ACCION ");
                    query.append("  ,ES_IPC ");
                    query.append("  ,ESCALA ");
                    query.append("  ,PI_GARANTE ");
                    query.append("  ,REGISTRO_PUBLICO_PROPIEDAD ");
                    query.append("  ,LEI ");
                    
                    query.append(", PRELACION_GAR");
                    query.append(", FOLIO_REF_FACTURA");
                    query.append(", RUORL");
                    query.append(", ROEEFM");
                    query.append(", FECHA_ULT_AVALUO");
                    query.append(", FECHA_INSCRIPCION_RPPC");
                    query.append(", FECHA_INSCRIPCION_RUG");
                    query.append(", FECHA_INSCRIPCION_RUORL");
                    query.append(", FECHA_INSCRIPCION_ROEEFM");
                    query.append(", LOCALIDAD_GARANTIA_INM");
                    //query.append(", HFX");
                    query.append(", FECHA_INFO");
                    
                    query.append("  ,STATUS ");
                    query.append("  ,ID_USUARIO_MODIFICACION ");
                    query.append("  ,MAC_ADDRESS_MODIFICACION ");
                    query.append("  ) ");
                    query.append("SELECT NUMERO_INSTITUCION ");
                    query.append("  ,NUMERO_GARANTIA ");
                    query.append("  ,'").append(fechaModificacion).append("'");
                    query.append("  ,ENDOSO");
                    query.append("  ,FECHA_ULT_ACT_GAR");
                    query.append("  ,FECHA_REGISTRO_RUG");
                    query.append("  ,TIPO_SEGURO");
                    query.append("  ,FECHA_ALTA_SEGURO");
                    query.append("  ,MONTO_SEGURO");
                    query.append("  ,NUMERO_CONTRATO_GAR_LIQ");
                    query.append("  ,CARTA_MANDATO_GAR_LIQ");
                    query.append("  ,TIPO_COBERTURA");
                    query.append("  ,CLAS_PROVEEDOR_PROT");
                    query.append("  ,NOMBRE_OBLIGADO");
                    query.append("  ,MONTO_OBLIGADO");
                    query.append("  ,NOMBRE_AVAL");
                    query.append("  ,MONTO_AVAL");
                    query.append("  ,TIPO_GARANTIA ");
                    query.append("  ,TIPO_GARANTIA_MA ");
                    query.append("  ,BANCO_GARANTIA ");
                    query.append("  ,VALOR_GARANTIA_PROYECTADO ");
                    query.append("  ,REG_GARANTIA_MOBILIARIA ");
                    query.append("  ,IND_REAL_FINANCIERA ");
                    query.append("  ,IND_FACTOR_HC ");
                    query.append("  ,HC ");
                    query.append("  ,IND_CALCULO_HC ");
                    query.append("  ,VENCIMIENTO_RESTANTE ");
                    query.append("  ,GRADO_RIESGO ");
                    query.append("  ,AGENCIA_CALIFICADORA ");
                    query.append("  ,CALIFICACION ");
                    query.append("  ,EMISOR ");
                    query.append("  ,ES_ACCION ");
                    query.append("  ,ES_IPC ");
                    query.append("  ,ESCALA ");
                    query.append("  ,PI_GARANTE ");
                    query.append("  ,REGISTRO_PUBLICO_PROPIEDAD ");
                    query.append("  ,LEI ");
                    
                    query.append(", PRELACION_GAR");
                    query.append(", FOLIO_REF_FACTURA");
                    query.append(", RUORL");
                    query.append(", ROEEFM");
                    query.append(", FECHA_ULT_AVALUO");
                    query.append(", FECHA_INSCRIPCION_RPPC");
                    query.append(", FECHA_INSCRIPCION_RUG");
                    query.append(", FECHA_INSCRIPCION_RUORL");
                    query.append(", FECHA_INSCRIPCION_ROEEFM");
                    query.append(", LOCALIDAD_GARANTIA_INM");
                    //query.append(", HFX");
                    query.append(", FECHA_INFO");
                    
                    query.append("  ,STATUS ");
                    query.append("  ,'").append(idUsuario).append("'");
                    query.append("  ,'").append(macAddress).append("'");
                    query.append("FROM SICC_FALTANTES_GARANTIAS ");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  AND NUMERO_GARANTIA = ").append(numeroGarantia);
                    System.out.println(query);
                    bd.executeUpdate(query.toString());

                    query.setLength(0);
                    query.append("UPDATE SICC_FALTANTES_GARANTIAS ");
                    query.append("SET ");
                    query.append(updateCommon.armaUpdate(listCampos, stD));
                    query.append("  ,FECHA_MODIFICACION = '").append(fechaModificacion).append("'");
                    query.append("  ,ID_USUARIO_MODIFICACION = '").append(idUsuario).append("'");
                    query.append("  ,MAC_ADDRESS_MODIFICACION = '").append(macAddress).append("'");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  AND NUMERO_GARANTIA = ").append(numeroGarantia);
                    System.out.println(query);
                    bd.executeUpdate(query.toString());

                    bd.commit();
                } catch (SQLException e) {
                    bd.rollback();
                    query.setLength(0);
                    query.append("UPDATE SICC_GARANTIA_LO ");
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
}

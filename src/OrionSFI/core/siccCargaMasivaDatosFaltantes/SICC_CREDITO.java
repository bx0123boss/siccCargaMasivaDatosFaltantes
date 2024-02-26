/*
 **********************************************************************************************************
 *    FECHA         ID.REQUERIMIENTO            DESCRIPCIÓN                            MODIFICÓ.          
 * 09/02/2015             3226            Creación del documento con            José Manuel Zúñiga Flores 
 *                                        especificaciones de la Carga de       Fernando Vázquez Galán    
 *                                        Archivo para Datos Faltantes de                                 
 *                                        Calificación de Cartera                                         
 *                                                                                                        
 * 
 * 
 * 15/01/2016           4920             Se cambian reglas de extracción de layouts               Uriel Caame
 *                                       Persona, Crédito, Anexo 20, 21, 22 y Aval
 *                                       Se eliminan campos de los layouts Persona, Crédito y Aval
 *                                       Se agregan campos al layout de Línea Crédito
 *                                       Cambio a nombre de estructura de Datos Faltantes 
 *                                       para layout de Linea Credito se crea layout 
 *                                       de faltantes para Garantías  se cambia tipo y 
 *                                       longitid de datos de layout de Info Financiera
 **********************************************************************************************************
 * 29/04/2016           5620        Adecuaciones Carga Masiva Datos Faltantes   José Manuel
 *                                  (Calificación de Cartera)
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
import java.util.*;

public class SICC_CREDITO {

    private JDBCConnectionPool bd = null;
    private StringBuffer query = null;
    private ValidaCampos validaCampos = null;
    private static int dosDecimales = 2;
    private static int cuatroDecimales = 4;
    private static int seisDecimales = 6;
    private static int ochoDecimales = 8;

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
        // expRegular.append("(\\d{1,5}+\\|)"); // NUMERO_CONSECUTIVO
        expRegular.append("(\\d{1,15}+\\|)"); // NUMERO_CUENTA
        expRegular.append("([a-zA-Z0-9-_ ]{0,12}+\\|)"); //CLASIFICACION_CONTABLE
        expRegular.append("(\\d{0,2}+\\|)"); //MOTIVO_CONDONACION
        expRegular.append("(\\d{0,21}+\\||\\d{0,21}+\\.+\\d{0,2}+\\|)"); //MONTO_DESCUENTOS
        expRegular.append("(\\d{0,21}+\\||\\d{0,21}+\\.+\\d{0,2}+\\|)"); //MONTO_OTROS
        expRegular.append("([a-zA-Z0-9-_ ]{0,250}+\\|)"); //NOMBRE_FACTORADO
        expRegular.append("([a-zA-Z0-9-_ ]{0,13}+\\|)"); //RFC_FACTORADO
        expRegular.append("(\\d{0,2}+\\|)"); //PROG_GOB_FED
        expRegular.append("(\\d{0,2}+\\|)"); //TIPO_CREDITO
        expRegular.append("(\\d{0,5}+\\|)"); //CLAVE_PREVENCION
        expRegular.append("(\\d{0,21}+\\||\\d{0,21}+\\.+\\d{0,2}+\\|)"); //ESTIMAC_ADIC_RIESGOS_OPERA
        expRegular.append("(\\d{0,21}+\\||\\d{0,21}+\\.+\\d{0,2}+\\|)"); //ESTIMAC_ADIC_CRED_VENC
        expRegular.append("(\\d{0,21}+\\||\\d{0,21}+\\.+\\d{0,2}+\\|)"); //ESTIMAC_ADIC_REC_CNBV
        
        //expRegular.append("(\\d{1,5}+\\|)"); // PRODUCTO_COMERCIAL
        //expRegular.append("(\\d{0,5}+\\|)"); // TIPO_CREDITO_R04A
        //expRegular.append("(\\d{1,3}+\\|)"); // TIPO_LINEA
        //expRegular.append("(\\d{0,3}+\\|)"); // TIPO_ALTA
//        expRegular.append("(\\d{0,3}+\\|)"); // GARANTIA_PERSONAL
//        expRegular.append("(\\d{0,1}+\\|)"); // INFO_RECUPERACION_CREDITO
        //expRegular.append("(\\d{1,3}+\\|)"); // POSICION
//        expRegular.append("((0|1){1,1}+\\|)"); // EXPEDIENTE_COMPLETO
//        expRegular.append("((0|1){1,1}+\\|)"); // OPERACION_FORMALIZADA
//        expRegular.append("(\\d{0,2}+\\|)"); // METODOLOGIA
        //expRegular.append("((0|1){1,1}+\\|)"); // REPORTADO_SIC
//        expRegular.append("((0|1){1,1}+\\|)"); // PARTICIPACION_FEDERAL
        //expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)"); // PORCENTAJE_PART_FEDERAL
        //expRegular.append("(\\d{0,15}+\\||\\d{0,15}+\\.+\\d{0,2}+\\|)"); // INTERES_REFINANCIADO
        //expRegular.append("(\\d{0,15}+[\\.]?+\\d{0,2}+\\|)"); // COMISIONES_DEVENGADAS
        //expRegular.append("(\\d{0,15}+[\\.]?+\\d{0,2}+\\|)"); // IMPORTE_QUEBRANTO
        //expRegular.append("(\\d{0,15}+\\||\\d{0,15}+\\.+\\d{0,2}+\\|)"); // EXPO_INCUMPLIMIENTO
        //expRegular.append("(\\d{0,15}+\\||\\d{0,15}+\\.+\\d{0,2}+\\|)"); // VAL_MERC_DERIV_CRED
        //expRegular.append("((0|1){1,1}+\\|)"); // CONCURSO_MERCANTIL
        //expRegular.append("((0|1){1,1}+\\|)"); // IND_EMPROBLEMADO
        //expRegular.append("((0|1){1,1}+\\|)"); // CUMPLE_CRITERIO_CONT
        //expRegular.append("([a-zA-Z0-9-_ ]{0,40}+\\|)"); // NUM_EMPRESTITOS_LOCAL
        //expRegular.append("([a-zA-Z0-9-_ ]{0,40}+\\|)"); // NUM_EMPRESTITOS_SHCP
        //expRegular.append("(\\d{0,15}+\\||\\d{0,15}+\\.+\\d{0,2}+\\|)"); // COMISIONES_COBRADAS
        //expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)"); // GATOS_ORIGINACION_TASA
        //expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)"); // COMISION_DISPOSICION_TASA
        //expRegular.append("(\\d{0,15}+\\||\\d{0,15}+\\.+\\d{0,2}+\\|)"); // COMISION_DISPOSICION_MONTO
        //expRegular.append("(\\d{0,15}+\\||\\d{0,15}+\\.+\\d{0,2}+\\|)"); // COMISION_ANUALIDAD
        //expRegular.append("(\\d{1,3}+\\|)"); // TIPO_OPERACION
        //expRegular.append("(\\d{0,3}+\\|)"); // TIPO_ALTA
        //expRegular.append("(\\d{0,3}+\\|)"); // TIPO_BAJA
        //expRegular.append("(\\d{0,3}+\\|)"); // TIPO_ALTA_MA
        //expRegular.append("(\\d{0,3}+\\|)"); // TIPO_BAJA_MA
        //expRegular.append("([a-zA-Z0-9-_ ]{1,18}+\\|)"); // FOLIO_CONSULTA_BURO
        //expRegular.append("(\\d{0,1}+\\|)"); // MITIGANTE
        //expRegular.append("((0|1){1,1}+\\|)"); // ES_PADRE
        //expRegular.append("((0|1){1,1}+\\|)"); // VIGENCIA_INDEFINIDA
        //expRegular.append("((0|1){1,1}+\\|)"); // CREDITO_SINDICADO
        //expRegular.append("(\\d{0,6}+\\|)"); // INSTITUTO_FONDEA
        //expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)"); // PORCENTAJE_GTIA_FONDO
        //expRegular.append("(\\d{0,15}+\\||\\d{0,15}+\\.+\\d{0,2}+\\|)"); // APOYO_BANCA_DESARROLLO
        //expRegular.append("(\\d{0,15}+\\||\\d{0,15}+\\.+\\d{0,2}+\\|)"); // SDO_QUITAS
        //expRegular.append("(\\d{0,15}+\\||\\d{0,15}+\\.+\\d{0,2}+\\|)"); // SDO_CASTIGOS
        //expRegular.append("(\\d{0,15}+\\||\\d{0,15}+\\.+\\d{0,2}+\\|)"); // SDO_CONDONACION
        //expRegular.append("(\\d{0,15}+\\||\\d{0,15}+\\.+\\d{0,2}+\\|)"); // SDO_DESCUENTOS
        //expRegular.append("(\\d{0,15}+\\||\\d{0,15}+\\.+\\d{0,2}+\\|)"); // SDO_DACION
        //expRegular.append("(\\d{0,15}+\\||\\d{0,15}+\\.+\\d{0,2}+\\|)"); // SDO_BONIFICACION
        //expRegular.append("([a-zA-Z0-9-_ ]{0,250}+\\|)"); // DEUDOR_FACTORAJE
        //expRegular.append("([a-zA-Z0-9-_ ]{0,13}+\\|)"); // RFC_DEUDOR_FACTORAJE
        //expRegular.append("(\\d{0,2})"); // DESTINO_CREDITO_MA

        if (registro.toString().matches(expRegular.toString())) {
            valido = true;
        }

        return valido;
    }

    public String separaRegistros(String registro, int consecutivo, int noLayout, String folioLote, short numeroInstitucion, String idUsuario, String macAddress) throws Exception {
        validaCampos = new ValidaCampos(numeroInstitucion, idUsuario, macAddress, folioLote);
        InspectorDatos iData = new InspectorDatos();
        StringBuffer errRegistro = new StringBuffer();
        char pipe = '|';
        errRegistro.setLength(0);
        String[] cad = registro.split("\\|");
        StringTokenizer st = new StringTokenizer(registro, "|");

        errRegistro.append(consecutivo).append(pipe);

        if (iData.isStringNULLExt(cad[0])) {
        	if(cad[0].equalsIgnoreCase("NULL"))
        		errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NUMERO_CUENTA", 15, noLayout, consecutivo, (byte) 1)).append(pipe);
        	else
        		errRegistro.append(validaCampos.getCampoNumerico(cad[0], "NUMERO_CUENTA", 15, noLayout, consecutivo, (byte) 1)).append(pipe);
        } else {
            errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NUMERO_CUENTA", 15, noLayout, consecutivo, (byte) 1)).append(pipe);
        }

        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Clasificacion Contable", 12, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Motivo Condonacion", 2, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Monto por Descuentos", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Monto de Otros Aumentos o Decrementos", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Nombre de Factorado", 250, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "RFC de Factorado", 13, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Gobierno Federal", 2, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Tipo de Credito", 2, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Clave de Prevencion", 5, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Monto Estimaciones Adic. por Riesgos Operativos", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Monto Estimaciones Adic. De Creditos Vencidos", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Monto Estimaciones Adic. Reconocidas CNBV", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "PRODUCTO_COMERCIAL", 5, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TIPO_CREDITO_R04A", 5, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TIPO_LINEA", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TIPO_ALTA", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
//        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "GARANTIA_PERSONAL", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
//        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "INFO_RECUPERACION_CREDITO", 1, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "POSICION", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
//        errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "EXPEDIENTE_COMPLETO", 1, noLayout, consecutivo, "0", (byte)1)).append(pipe);
//        errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "OPERACION_FORMALIZADA", 1, noLayout, consecutivo, "0", (byte)1)).append(pipe);
//        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "METODOLOGIA", 2, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "REPORTADO_SIC", 1, noLayout, consecutivo, "0", (byte) 1)).append(pipe);
//        errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "PARTICIPACION_FEDERAL", 1, noLayout, consecutivo, "0", (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PORCENTAJE_PART_FEDERAL", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "INTERES_REFINANCIADO", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "COMISIONES_DEVENGADAS", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "IMPORTE_QUEBRANTO", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "EXPO_INCUMPLIMIENTO", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "VAL_MERC_DERIV_CRED", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "CONCURSO_MERCANTIL", 1, noLayout, consecutivo, "0", (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "IND_EMPROBLEMADO", 1, noLayout, consecutivo, "0", (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "CUMPLE_CRITERIO_CONT", 1, noLayout, consecutivo, "0", (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "NUM_EMPRESTITOS_LOCAL", 40, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "NUM_EMPRESTITOS_SHCP", 40, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "COMISIONES_COBRADAS", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "GASTOS_ORIGINACION_TASA", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "COMISION_DISPOSICION_TASA", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "COMISION_DISPOSICION_MONTO", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "COMISION_ANUALIDAD", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TIPO_OPERACION", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TIPO_ALTA", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TIPO_BAJA", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TIPO_ALTA_MA", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TIPO_BAJA_MA", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "FOLIO_CONSULTA_BURO", 18, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "MITIGANTE", 1, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "ES_PADRE", 1, noLayout, consecutivo, "0", (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "VIGENCIA_INDEFINIDA", 1, noLayout, consecutivo, "0", (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "CREDITO_SINDICADO", 1, noLayout, consecutivo, "0", (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "INSTITUTO_FONDEA", 6, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PORCENTAJE_GTIA_FONDO", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "APOYO_BANCA_DESARROLLO", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SDO_QUITAS", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SDO_CASTIGOS", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SDO_CONDONACION", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SDO_DESCUENTOS", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SDO_DACION", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SDO_BONIFICACION", 17, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "DEUDOR_FACTORAJE", 250, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "RFC_DEUDOR_FACTORAJE", 13, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "DESTINO_CREDITO_MA", 2, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "OPER_DIF_TASA_DISP", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "OTROS_MONTOS_PAGADOS", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "ETAPA_DETERIORIO_NIFC16", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "CLASE_EFM", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "IND_DEUDA_ESTATAL", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "HIPOTESIS_PRESUNCION", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "SEGMENTO_NIFC16", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NOMBRE_SISTEMA_NIFC16", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "INDICADOR_MI_NIFC16", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "CALIFICACION_NIFC16", 10, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "RESERVAS_MI_C16_12MESES", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "RESERVAS_MI_C16_LIFETIME", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SP_MI_C16", 16, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "EI_MI_C16", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "FACT_CONV_CRED_NIFC16", 9, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "TASA_DESC_NIFC16", 16, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TASA_INTERES_NIFC16", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "TASA_PREPAGO_NIFC16", 5, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PLAZO_LIFETIME_NIFC16", 7, noLayout, consecutivo, cuatroDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PLAZO_ORIGINAL_NIFC16", 7, noLayout, consecutivo, cuatroDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PLAZO_REMANENTE_NIFC16", 7, noLayout, consecutivo, cuatroDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "HIPOTESIS_PRESUNCION_NIFC16", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "SEGMENTO_MI", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NOMBRE_SISTEMA_MI", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "IND_ENFOQUE_MI", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "INDICADOR_MI", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "CALIFICACION_MI", 10, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SP_MI", 16, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "EI_MI", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "VENCIMIENTO_MI", 6, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "CORRELACION_MI", 16, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "POND_REQ_CAP_MI", 16, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "REQ_CAP_MI", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "PORCENTAJE_PISO_MI", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "VALUACION_DERIVADO_CRED", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "MON_VALUACION_DERIVADO", 2, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "ID_PPP", 40, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "ID_PYM", 40, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "FUENTE_PAGO", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SALDO_DEUDA", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "FONDO_RESERVA_EYM", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "AGENCIA_CAL_EYM", 6, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "CALIFICACION_EYM", 8, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "CLAS_CONTABLE_417", 12, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "PE_CE", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "RES_12MESES", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "RES_GAR_12MESES", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "RES_CLIENTE_12MESES", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "RES_LIFETIME", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "RES_TOTALES", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "RES_CONSTITUIDAS", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "RES_DESCONSTITUIDAS", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PI_TOTAL", 18, noLayout, consecutivo, ochoDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PI_GARANTE", 18, noLayout, consecutivo, ochoDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PI_ACREDITADO", 18, noLayout, consecutivo, ochoDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SP_TOTAL", 18, noLayout, consecutivo, ochoDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SP_GARANTE", 18, noLayout, consecutivo, ochoDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SP_ACREDITADO", 18, noLayout, consecutivo, ochoDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "EI_TOTAL", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "EI_GARANTE", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "EI_ACREDITADO", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PLAZO_ORIGINAL", 7, noLayout, consecutivo, cuatroDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PLAZO_REMANENTE", 7, noLayout, consecutivo, cuatroDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "SUST_PI", 1, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "MESES_PI_100", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "GRADO_RIESGO", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "RESERVAS_ADIC_CONSTITUIDAS", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "RESERVAS_ADIC_DESCONSTITUIDAS", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "RESERVAS_MI", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "FACTOR_CONV_RIESGO", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "EXP_AJUSTADA_MIT", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "EXP_NETA_RES", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "POND_RIESGO", 10, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "REQ_CAP_CRED", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "POR_NO_CUBIERTO", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SP_NO_CUBIERTO", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "POR_GAR_REAL_FIN", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SP_AJUST_GAR_REAL_FIN", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "EI_AJUST_GAR_REAL", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "POR_GAR_REAL_NO_FIN", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SP_DER_COBRO", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SP_B_INMUEBLES", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SP_B_MUEBLES", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SP_FID_GAR", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SP_FID_ING_PROPIOS", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SP_OTROS_REAL_NO_FIN", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SP_AJUST_GAR_REAL_NO_FIN", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "POR_CUB_GAR_PER", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "POR_CUB_OBLIGADO", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "MONTO_CUB_GP", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "POR_CUB_PYM", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "POR_CUB_PP", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "MONTO_CUB_PYM", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "MONTO_CUB_PP", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "RCSD", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PE_EYM", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "FECHA_INFO", 10, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "ID_PROYECTO", 22, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "RC_CALIFICACION", 8, noLayout, consecutivo, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "VALOR_MITIGANTE", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Moneda Mitigante", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Codigo Credito Reestructurado", 22, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Tamaño Acreditado", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Saldo Baja", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Monto Pagado Baja", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Exposici�n Incumplimiento Expuesta", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Numero Garantias Reales Financieras", 2, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Factor Ajuste", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Valor Contable Garant�a Real Financiera", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Factor Ajuste No Financieras", 2, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Garantia Derechos Cobro", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Garantia Bien Inmueble", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Garantia Bien Mueble", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Garantia Fideicomiso y Aportaciones Federales", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), " Garantia Fideicomiso e Ingresos Propios", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Garantia Otras Garantias Reales No Financieras", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Numero Garantias Personales", 2, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Sustitucion Probabilidad Incumplimiento", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        //errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Probabilidad Incumplimiento", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);


             
        
        return errRegistro.toString();
    }

    public void insertaCarga(String registro, short numeroInstitucion, String nombreArchivo, String folioLote, int noLayout, String idUsuario, String macAddress) throws Exception {
        MensajesSistema msjSistema = new MensajesSistema();
        SQLProperties sqlProperties = new SQLProperties();
        validaCampos = new ValidaCampos();

        SimpleDateFormat sdf = new SimpleDateFormat(sqlProperties.getFormatoSoloFechaOrion());
        String fechaAlta = sqlProperties.getFechaInsercionFormateada(sdf.format(new Date()));
        InspectorDatos iDat = new InspectorDatos();
        StringTokenizer st = new StringTokenizer(registro, "|");
        byte numero = 0;
        byte cadena = 1;
        try {

            bd = new JDBCConnectionPool();

            query = new StringBuffer();
            query.append("INSERT INTO dbo.SICC_CREDITO_LO(");
            query.append("  NUMERO_INSTITUCION");
            query.append(" ,NOMBRE_ARCHIVO");
            query.append(" ,FOLIO_LOTE");
            query.append(" ,NUMERO_LAYOUT");
            query.append(" ,NUMERO_CONSECUTIVO");
            query.append(" ,NUMERO_CUENTA");
            query.append(" ,CLASIFICACION_CONTABLE");
            query.append(" ,MOTIVO_CONDONACION");
            query.append(" ,MONTO_DESCUENTOS");
            query.append(" ,MONTO_OTROS");
            query.append(" ,NOMBRE_FACTORADO");
            query.append(" ,RFC_FACTORADO");
            query.append(" ,PROG_GOB_FED");
            query.append(" ,TIPO_CREDITO");
            query.append(" ,CLAVE_PREVENCION");
            query.append(" ,ESTIMAC_ADIC_RIESGOS_OPERA");
            query.append(" ,ESTIMAC_ADIC_CRED_VENC");
            query.append(" ,ESTIMAC_ADIC_REC_CNBV");
            query.append(" ,PRODUCTO_COMERCIAL");
            //query.append(", TIPO_CREDITO_R04A");
            query.append(" ,TIPO_LINEA");
            //query.append(", TIPO_ALTA");
            query.append(" ,GARANTIA_PERSONAL");
            query.append(" ,INFO_RECUPERACION_CREDITO");
            query.append(" ,POSICION");
            query.append(" ,EXPEDIENTE_COMPLETO");
            query.append(" ,OPERACION_FORMALIZADA");
            query.append(" ,METODOLOGIA");
            query.append(" ,REPORTADO_SIC");
            query.append(" ,PARTICIPACION_FEDERAL");
            query.append(" ,PORCENTAJE_PART_FEDERAL");
            query.append(" ,INTERES_REFINANCIADO");
            //query.append(", COMISIONES_DEVENGADAS");
            //query.append(", IMPORTE_QUEBRANTO");
            query.append(" ,EXPO_INCUMPLIMIENTO");
            query.append(" ,VAL_MERC_DERIV_CRED");
            query.append(" ,CONCURSO_MERCANTIL");
            query.append(" ,IND_EMPROBLEMADO");
            query.append(" ,CUMPLE_CRITERIO_CONT");
            query.append(" ,NUM_EMPRESTITOS_LOCAL");
            query.append(" ,NUM_EMPRESTITOS_SHCP");
            query.append(" ,COMISIONES_COBRADAS");
            query.append(" ,GASTOS_ORIGINACION_TASA");
            query.append(" ,COMISION_DISPOSICION_TASA");
            query.append(" ,COMISION_DISPOSICION_MONTO");
            query.append(" ,COMISION_ANUALIDAD");

            query.append(" ,TIPO_OPERACION");
            query.append(" ,TIPO_ALTA");
            query.append(" ,TIPO_BAJA");
            query.append(" ,TIPO_ALTA_MA");
            query.append(" ,TIPO_BAJA_MA");
            query.append(" ,FOLIO_CONSULTA_BURO");
            query.append(" ,MITIGANTE");
            query.append(" ,ES_PADRE");
            query.append(" ,VIGENCIA_INDEFINIDA");
            query.append(" ,CREDITO_SINDICADO");
            query.append(" ,INSTITUTO_FONDEA");
            query.append(" ,PORCENTAJE_GTIA_FONDO");
            query.append(" ,APOYO_BANCA_DESARROLLO");
            query.append(" ,SDO_QUITAS");
            query.append(" ,SDO_CASTIGOS");
            query.append(" ,SDO_CONDONACION");
            query.append(" ,SDO_DESCUENTOS");
            query.append(" ,SDO_DACION");
            query.append(" ,SDO_BONIFICACION");
            query.append(" ,DEUDOR_FACTORAJE");
            query.append(" ,RFC_DEUDOR_FACTORAJE");
            query.append(" ,DESTINO_CREDITO_MA");
            
            query.append(" ,OPER_DIF_TASA_DISP");
            query.append(" ,OTROS_MONTOS_PAGADOS");
            query.append(" ,ETAPA_DETERIORIO_NIFC16");
            query.append(" ,CLASE_EFM");
            query.append(" ,IND_DEUDA_ESTATAL");
            query.append(" ,HIPOTESIS_PRESUNCION");
            query.append(" ,SEGMENTO_NIFC16");
            query.append(" ,NOMBRE_SISTEMA_NIFC16");
            query.append(" ,INDICADOR_MI_NIFC16");
            query.append(" ,CALIFICACION_NIFC16");
            query.append(" ,RESERVAS_MI_C16_12MESES");
            query.append(" ,RESERVAS_MI_C16_LIFETIME");
            query.append(" ,SP_MI_C16");
            query.append(" ,EI_MI_C16");
            query.append(" ,FACT_CONV_CRED_NIFC16");
            query.append(" ,TASA_DESC_NIFC16");
            query.append(" ,TASA_INTERES_NIFC16");
            query.append(" ,TASA_PREPAGO_NIFC16");
            query.append(" ,PLAZO_LIFETIME_NIFC16");
            query.append(" ,PLAZO_ORIGINAL_NIFC16");
            query.append(" ,PLAZO_REMANENTE_NIFC16");
            query.append(" ,HIPOTESIS_PRESUNCION_NIFC16");
            query.append(" ,SEGMENTO_MI");
            query.append(" ,NOMBRE_SISTEMA_MI");
            query.append(" ,IND_ENFOQUE_MI");
            query.append(" ,INDICADOR_MI");
            query.append(" ,CALIFICACION_MI");
            query.append(" ,SP_MI");
            query.append(" ,EI_MI");
            query.append(" ,VENCIMIENTO_MI");
            query.append(" ,CORRELACION_MI");
            query.append(" ,POND_REQ_CAP_MI");
            query.append(" ,REQ_CAP_MI");
            query.append(" ,PORCENTAJE_PISO_MI");
            query.append(" ,VALUACION_DERIVADO_CRED");
            query.append(" ,MON_VALUACION_DERIVADO");
            query.append(" ,ID_PPP");
            query.append(" ,ID_PYM");
            query.append(" ,FUENTE_PAGO");
            query.append(" ,SALDO_DEUDA");
            query.append(" ,FONDO_RESERVA_EYM");
            query.append(" ,AGENCIA_CAL_EYM");
            query.append(" ,CALIFICACION_EYM");
            query.append(" ,CLAS_CONTABLE_417");
            query.append(" ,PE_CE");
            query.append(" ,RES_12MESES");
            query.append(" ,RES_GAR_12MESES");
            query.append(" ,RES_CLIENTE_12MESES");
            query.append(" ,RES_LIFETIME");
            query.append(" ,RES_TOTALES");
            query.append(" ,RES_CONSTITUIDAS");
            query.append(" ,RES_DESCONSTITUIDAS");
            query.append(" ,PI_TOTAL");
            query.append(" ,PI_GARANTE");
            query.append(" ,PI_ACREDITADO");
            query.append(" ,SP_TOTAL");
            query.append(" ,SP_GARANTE");
            query.append(" ,SP_ACREDITADO");
            query.append(" ,EI_TOTAL");
            query.append(" ,EI_GARANTE");
            query.append(" ,EI_ACREDITADO");
            query.append(" ,PLAZO_ORIGINAL");
            query.append(" ,PLAZO_REMANENTE");
            query.append(" ,SUST_PI");
            query.append(" ,MESES_PI_100");
            query.append(" ,GRADO_RIESGO");
            query.append(" ,RESERVAS_ADIC_CONSTITUIDAS");
            query.append(" ,RESERVAS_ADIC_DESCONSTITUIDAS");
            query.append(" ,RESERVAS_MI");
            query.append(" ,FACTOR_CONV_RIESGO");
            query.append(" ,EXP_AJUSTADA_MIT");
            query.append(" ,EXP_NETA_RES");
            query.append(" ,POND_RIESGO");
            query.append(" ,REQ_CAP_CRED");
            query.append(" ,POR_NO_CUBIERTO");
            query.append(" ,SP_NO_CUBIERTO");
            query.append(" ,POR_GAR_REAL_FIN");
            query.append(" ,SP_AJUST_GAR_REAL_FIN");
            query.append(" ,EI_AJUST_GAR_REAL");
            query.append(" ,POR_GAR_REAL_NO_FIN");
            query.append(" ,SP_DER_COBRO");
            query.append(" ,SP_B_INMUEBLES");
            query.append(" ,SP_B_MUEBLES");
            query.append(" ,SP_FID_GAR");
            query.append(" ,SP_FID_ING_PROPIOS");
            query.append(" ,SP_OTROS_REAL_NO_FIN");
            query.append(" ,SP_AJUST_GAR_REAL_NO_FIN");
            query.append(" ,POR_CUB_GAR_PER");
            query.append(" ,POR_CUB_OBLIGADO");
            query.append(" ,MONTO_CUB_GP");
            query.append(" ,POR_CUB_PYM");
            query.append(" ,POR_CUB_PP");
            query.append(" ,MONTO_CUB_PYM");
            query.append(" ,MONTO_CUB_PP");
            query.append(" ,RCSD");
            query.append(" ,PE_EYM");
            query.append(" ,FECHA_INFO");
            query.append(" ,ID_PROYECTO");
            
            query.append(" ,RC_CALIFICACION");
            query.append(" ,VALOR_MITIGANTE");
            query.append(" ,MONEDA_MITIGANTE");
            query.append(" ,CODIGOCREDITOREESTRUCTURA");
            query.append(" ,TAMANO_ACREDITADO");
            query.append(" ,SALDO_BAJA");
            query.append(" ,MONTO_PAGADO_BAJA");
            query.append(" ,EI_EXPUESTA");
            query.append(" ,NUM_GAR_REAL_FIN");
            query.append(" ,HFX");
            query.append(" ,VAL_CONT_GAR_REAL_FIN");
            query.append(" ,NUM_GAR_REAL_NO_FIN");
            query.append(" ,GTIA_DER_COBRO");
            query.append(" ,GTIA_BIEN_INMUEBLE");
            query.append(" ,GTIA_BIEN_MUEBLE");
            query.append(" ,GTIA_FIDEICOM_EYM");
            query.append(" ,GTIA_FIDEICOM_ING_PROP");
            query.append(" ,GTIA_OTROS_REAL_NO_FIN");
            query.append(" ,NUM_GAR_PERS");
            query.append(" ,SUST_PI_2");
            query.append(" ,PI_ASEG");

            query.append(" ,STATUS_CARGA");
            query.append(" ,STATUS_ALTA");
            query.append(" ,STATUS");
            query.append(" ,FECHA_ALTA");
            query.append(" ,ID_USUARIO_ALTA");
            query.append(" ,MAC_ADDRESS_ALTA");
            query.append(" ,FECHA_MODIFICACION");
            query.append(" ,ID_USUARIO_MODIFICACION");
            query.append(" ,MAC_ADDRESS_MODIFICACION) ");
            query.append("VALUES(");
            query.append("  ").append(numeroInstitucion);
            query.append(", '").append(nombreArchivo).append("'");
            query.append(", '").append(folioLote).append("'");
            query.append(", ").append(noLayout);
            query.append(", ").append(st.nextToken());                              //NUMERO_CONSECUTIVO           
            query.append(", ").append(st.nextToken());                              //NUMERO_CUENTA
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //CLASIFICACION_CONTABLE
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //MOTIVO_CONDONACION
            query.append(", ").append(validaCampos.convierteNullCero(st.nextToken(), numero)); //MONTO_DESCUENTOS
            query.append(", ").append(validaCampos.convierteNullCero(st.nextToken(), numero)); //MONTO_OTROS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //NOMBRE_FACTORADO
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //RFC_FACTORADO
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //PROG_GOB_FED
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //TIPO_CREDITO
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //CLAVE_PREVENCION
            query.append(", ").append(validaCampos.convierteNullCero(st.nextToken(), numero)); //ESTIMAC_ADIC_RIESGOS_OPERA
            query.append(", ").append(validaCampos.convierteNullCero(st.nextToken(), numero)); //ESTIMAC_ADIC_CRED_VENC
            query.append(", ").append(validaCampos.convierteNullCero(st.nextToken(), numero)); //ESTIMAC_ADIC_REC_CNBV
            

            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//PRODUCTO_COMERCIAL
            query.append(", NULL");//PRODUCTO_COMERCIAL
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//TIPO_CREDITO_R04A
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//TIPO_LINEA
            query.append(", NULL");//TIPO_LINEA
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//TIPO_ALTA
//            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//GARANTIA_PERSONAL
            query.append(", NULL");//GARANTIA_PERSONAL
//            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//INFO_RECUPERACION_CREDI
            query.append(", NULL");//INFO_RECUPERACION_CREDI
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//POSICION
            query.append(", NULL");//POSICION
//            query.append(", ").append(st.nextToken());                              //EXPEDIENTE_COMPLETO
            query.append(", NULL");                              //EXPEDIENTE_COMPLETO
//            query.append(", ").append(st.nextToken());                              //OPERACIÓN_FORMALIZADA
            query.append(", NULL");                              //OPERACIÓN_FORMALIZADA
//            query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//METODOLOGIA
            query.append(", NULL");//METODOLOGIA
            //query.append(", ").append(st.nextToken());                              //REPORTADO_SIC
            query.append(", NULL");//REPORTADO_SIC
//            query.append(", ").append(st.nextToken());                              //PARTICIPACION_FEDERAL
            query.append(", NULL");                              //PARTICIPACION_FEDERAL
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//PORCENTAJE_PART_FEDERAL
            query.append(", NULL");//PORCENTAJE_PART_FEDERAL
            //query.append(", ").append(validaCampos.convierteNullCero(st.nextToken(), numero));//INTERES_REFINANCIADO
            query.append(", NULL");//INTERES_REFINANCIADO
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//COMISIONES_DEVENGADAS
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//IMPORTE_QUEBRANTO
            //query.append(", ").append(validaCampos.convierteNullCero(st.nextToken(), numero));//EXPO_INCUMPLIMIENTO
            query.append(", NULL");//EXPO_INCUMPLIMIENTO
            //query.append(", ").append(validaCampos.convierteNullCero(st.nextToken(), numero));//VAL_MERC_DERIV_CRED
            query.append(", NULL");//VAL_MERC_DERIV_CRED
            //query.append(", ").append(st.nextToken());                              //CONCURSO_MERCANTIL
            query.append(", NULL");//CONCURSO_MERCANTIL
            //query.append(", ").append(st.nextToken());                              //IND_EMPROBLEMADO
            query.append(", NULL");//IND_EMPROBLEMADO
            //query.append(", ").append(st.nextToken());                              //CUMPLE_CRITERIO_CONT
            query.append(", NULL");//CUMPLE_CRITERIO_CONT
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), cadena));//NUM_EMPRESTITOS_LOCAL
            query.append(", NULL");//NUM_EMPRESTITOS_LOCAL
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), cadena));//NUM_EMPRESTITOS_SHCP
            query.append(", NULL");//NUM_EMPRESTITOS_SHCP
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//COMISIONES_COBRADAS
            query.append(", NULL");//COMISIONES_COBRADAS
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//GASTOS_ORIGINACION_TASA
            query.append(", NULL");//GASTOS_ORIGINACION_TASA
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//COMISION_DISPOSICION_TASA
            query.append(", NULL");//COMISION_DISPOSICION_TASA
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//COMISION_DISPOSICION_MONTO
            query.append(", NULL");//COMISION_DISPOSICION_MONTO
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));//COMISION_ANUALIDAD
            query.append(", NULL");//COMISION_ANUALIDAD
            //query.append(", ").append(st.nextToken()); // TIPO_OPERACION
            query.append(", NULL");// TIPO_OPERACION
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // TIPO_ALTA
            query.append(", NULL");// TIPO_ALTA
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // TIPO_BAJA
            query.append(", NULL");// TIPO_BAJA
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // TIPO_ALTA_MA
            query.append(", NULL");// TIPO_ALTA_MA
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // TIPO_BAJA_MA
            query.append(", NULL");// TIPO_BAJA_MA
           
            //String folioConsulta= st.nextToken();
            //query.append(", ").append(validaCampos.validaStringNull(iDat.isStringNULLExt(folioConsulta)?"&#":folioConsulta, cadena)); // FOLIO_CONSULTA_BURO
            query.append(", NULL");// FOLIO_CONSULTA_BURO
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // MITIGANTE
            query.append(", NULL");// MITIGANTE
            //query.append(", ").append(st.nextToken()); // ES_PADRE
            query.append(", NULL");// ES_PADRE
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // VIGENCIA_INDEFINIDA
            query.append(", NULL");// VIGENCIA_INDEFINIDA
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // CREDITO_SINDICADO
            query.append(", NULL");// CREDITO_SINDICADO
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // INSTITUTO_FONDEA
            query.append(", NULL");// INSTITUTO_FONDEA
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // PORCENTAJE_GTIA_FONDO
            query.append(", NULL");// PORCENTAJE_GTIA_FONDO
            //query.append(", ").append(validaCampos.convierteNullCero(st.nextToken(), numero)); // APOYO_BANCA_DESARROLLO
            query.append(", NULL");// APOYO_BANCA_DESARROLLO
            //query.append(", ").append(validaCampos.convierteNullCero(st.nextToken(), numero)); // SDO_QUITAS
            query.append(", NULL");// SDO_QUITAS
            //query.append(", ").append(validaCampos.convierteNullCero(st.nextToken(), numero)); // SDO_CASTIGOS
            query.append(", NULL");// SDO_CASTIGOS
            //query.append(", ").append(validaCampos.convierteNullCero(st.nextToken(), numero)); // SDO_CONDONACION
            query.append(", NULL");// SDO_CONDONACION
            //query.append(", ").append(validaCampos.convierteNullCero(st.nextToken(), numero)); // SDO_DESCUENTOS
            query.append(", NULL");// SDO_DESCUENTOS
            //query.append(", ").append(validaCampos.convierteNullCero(st.nextToken(), numero)); // SDO_DACION
            query.append(", NULL");// SDO_DACION
            //query.append(", ").append(validaCampos.convierteNullCero(st.nextToken(), numero)); // SDO_BONIFICACION
            query.append(", NULL");// SDO_BONIFICACION
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // DEUDOR_FACTORAJE
            query.append(", NULL");// DEUDOR_FACTORAJE
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // RFC_DEUDOR_FACTORAJE
            query.append(", NULL");// RFC_DEUDOR_FACTORAJE
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // DESTINO_CREDITO_MA
            query.append(", NULL");// DESTINO_CREDITO_MA
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));// OPER_DIF_TASA_DISP
            query.append(", NULL");// OPER_DIF_TASA_DISP
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));// OTROS_MONTOS_PAGADOS
            query.append(", NULL");// OTROS_MONTOS_PAGADOS
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // ETAPA_DETERIORIO_NIFC16
            query.append(", NULL");// ETAPA_DETERIORIO_NIFC16
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));// CLASE_EFM
            query.append(", NULL");// CLASE_EFM
            //query.append(", ").append(st.nextToken());// IND_DEUDA_ESTATAL
            query.append(", NULL");// IND_DEUDA_ESTATAL
            //query.append(", ").append(st.nextToken());// HIPOTESIS_PRESUNCION
            query.append(", NULL");// HIPOTESIS_PRESUNCION
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // SEGMENTO_NIFC16
            query.append(", NULL");// SEGMENTO_NIFC16
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // NOMBRE_SISTEMA_NIFC16
            query.append(", NULL");// NOMBRE_SISTEMA_NIFC16
            //query.append(", ").append(st.nextToken());// INDICADOR_MI_NIFC16
            query.append(", NULL");// INDICADOR_MI_NIFC16
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // CALIFICACION_NIFC16
            query.append(", NULL");// CALIFICACION_NIFC16
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // RESERVAS_MI_C16_12MESES
            query.append(", NULL");// RESERVAS_MI_C16_12MESES
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // RESERVAS_MI_C16_LIFETIME
            query.append(", NULL");// RESERVAS_MI_C16_LIFETIME
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // SP_MI_C16
            query.append(", NULL");// SP_MI_C16
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // EI_MI_C16
            query.append(", NULL"); // EI_MI_C16
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // FACT_CONV_CRED_NIFC16
            query.append(", NULL");// FACT_CONV_CRED_NIFC16
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // TASA_DESC_NIFC16
            query.append(", NULL");// TASA_DESC_NIFC16
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // TASA_INTERES_NIFC16
            query.append(", NULL");// TASA_INTERES_NIFC16
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // TASA_PREPAGO_NIFC16
            query.append(", NULL"); // TASA_PREPAGO_NIFC16
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // PLAZO_LIFETIME_NIFC16
            query.append(", NULL"); // PLAZO_LIFETIME_NIFC16
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // PLAZO_ORIGINAL_NIFC16
            query.append(", NULL"); // PLAZO_ORIGINAL_NIFC16
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // PLAZO_REMANENTE_NIFC16
            query.append(", NULL");// PLAZO_REMANENTE_NIFC16
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // HIPOTESIS_PRESUNCION_NIFC16
            query.append(", NULL"); // HIPOTESIS_PRESUNCION_NIFC16
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // SEGMENTO_MI
            query.append(", NULL");// SEGMENTO_MI
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // NOMBRE_SISTEMA_MI
            query.append(", NULL"); // NOMBRE_SISTEMA_MI
            //query.append(", ").append(st.nextToken());// IND_ENFOQUE_MI
            query.append(", NULL"); // IND_ENFOQUE_MI
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // INDICADOR_MI
            query.append(", NULL"); // INDICADOR_MI
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // CALIFICACION_MI
            query.append(", NULL"); // CALIFICACION_MI
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // SP_MI
            query.append(", NULL"); // SP_MI
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // EI_MI
            query.append(", NULL"); // EI_MI
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // VENCIMIENTO_MI
            query.append(", NULL");// VENCIMIENTO_MI
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // CORRELACION_MI
            query.append(", NULL"); // CORRELACION_MI
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // POND_REQ_CAP_MI
            query.append(", NULL"); // POND_REQ_CAP_MI
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // REQ_CAP_MI
            query.append(", NULL"); // REQ_CAP_MI
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // PORCENTAJE_PISO_MI
            query.append(", NULL");// PORCENTAJE_PISO_MI
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // VALUACION_DERIVADO_CRED
            query.append(", NULL"); // VALUACION_DERIVADO_CRED
            //query.append(", ").append(st.nextToken());// MON_VALUACION_DERIVADO
            query.append(", NULL");// MON_VALUACION_DERIVADO
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // ID_PPP
            query.append(", NULL"); // ID_PPP
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // ID_PYM
            query.append(", NULL"); // ID_PYM
            //query.append(", ").append(st.nextToken());// FUENTE_PAGO
            query.append(", NULL"); // FUENTE_PAGO
            //query.append(", ").append(st.nextToken());// SALDO_DEUDA
            query.append(", NULL");// SALDO_DEUDA
            //query.append(", ").append(st.nextToken());// FONDO_RESERVA_EYM
            query.append(", NULL");// FONDO_RESERVA_EYM
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero)); // AGENCIA_CAL_EYM
            query.append(", NULL"); // AGENCIA_CAL_EYM
            //query.append(", ").append(st.nextToken());// CALIFICACION_EYM
            query.append(", NULL");// CALIFICACION_EYM
            //query.append(", ").append(st.nextToken());// CLAS_CONTABLE_417
            query.append(", NULL"); // CLAS_CONTABLE_417
            //query.append(", ").append(st.nextToken());// PE_CE
            query.append(", NULL"); // PE_CE
            //query.append(", ").append(st.nextToken());// RES_12MESES
            query.append(", NULL"); // RES_12MESES
            //query.append(", ").append(st.nextToken());// RES_GAR_12MESES
            query.append(", NULL"); // RES_GAR_12MESES
            //query.append(", ").append(st.nextToken());// RES_CLIENTE_12MESES
            query.append(", NULL");// RES_CLIENTE_12MESES
            //query.append(", ").append(st.nextToken());// RES_LIFETIME
            query.append(", NULL");// RES_LIFETIME
            //query.append(", ").append(st.nextToken());// RES_TOTALES
            query.append(", NULL");// RES_TOTALES
            //query.append(", ").append(st.nextToken());// RES_CONSTITUIDAS
            query.append(", NULL");// RES_CONSTITUIDAS
            //query.append(", ").append(st.nextToken());// RES_DESCONSTITUIDAS
            query.append(", NULL");// RES_DESCONSTITUIDAS
            //query.append(", ").append(st.nextToken());// PI_TOTAL
            query.append(", NULL");// PI_TOTAL
            //query.append(", ").append(st.nextToken());// PI_GARANTE
            query.append(", NULL");// PI_GARANTE
            //query.append(", ").append(st.nextToken());// PI_ACREDITADO
            query.append(", NULL");// PI_ACREDITADO
            //query.append(", ").append(st.nextToken());// SP_TOTAL
            query.append(", NULL");// SP_TOTAL
            //query.append(", ").append(st.nextToken());// SP_GARANTE
            query.append(", NULL");// SP_GARANTE
            //query.append(", ").append(st.nextToken());// SP_ACREDITADO
            query.append(", NULL");// SP_ACREDITADO
            //query.append(", ").append(st.nextToken());// EI_TOTAL
            query.append(", NULL");// EI_TOTAL
            //query.append(", ").append(st.nextToken());// EI_GARANTE
            query.append(", NULL");// EI_GARANTE
            //query.append(", ").append(st.nextToken());// EI_ACREDITADO
            query.append(", NULL");// EI_ACREDITADO
            //query.append(", ").append(st.nextToken());// PLAZO_ORIGINAL
            query.append(", NULL");// PLAZO_ORIGINAL
            //query.append(", ").append(st.nextToken());// PLAZO_REMANENTE
            query.append(", NULL");// PLAZO_REMANENTE
            //query.append(", ").append(st.nextToken());// SUST_PI
            query.append(", NULL");// SUST_PI
            //query.append(", ").append(st.nextToken());// MESES_PI_100
            query.append(", NULL");// MESES_PI_100
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), cadena));// GRADO_RIESGO
            query.append(", NULL");// GRADO_RIESGO
            //query.append(", ").append(st.nextToken());// RESERVAS_ADIC_CONSTITUIDAS
            query.append(", NULL");// RESERVAS_ADIC_CONSTITUIDAS
            //query.append(", ").append(st.nextToken());// RESERVAS_ADIC_DESCONSTITUIDAS
            query.append(", NULL");// RESERVAS_ADIC_DESCONSTITUIDAS
            //query.append(", ").append(st.nextToken());// RESERVAS_MI
            query.append(", NULL");// RESERVAS_MI
            //query.append(", ").append(st.nextToken());// FACTOR_CONV_RIESGO
            query.append(", NULL");// FACTOR_CONV_RIESGO
            //query.append(", ").append(st.nextToken());// EXP_AJUSTADA_MIT
            query.append(", NULL");// EXP_AJUSTADA_MIT
            //query.append(", ").append(st.nextToken());// EXP_NETA_RES
            query.append(", NULL");// EXP_NETA_RES
            //query.append(", ").append(st.nextToken());// POND_RIESGO
            query.append(", NULL");// POND_RIESGO
            //query.append(", ").append(st.nextToken());// REQ_CAP_CRED
            query.append(", NULL");// REQ_CAP_CRED
            //query.append(", ").append(st.nextToken());// POR_NO_CUBIERTO
            query.append(", NULL");// POR_NO_CUBIERTO
            //query.append(", ").append(st.nextToken());// SP_NO_CUBIERTO
            query.append(", NULL");// SP_NO_CUBIERTO
            //query.append(", ").append(st.nextToken());// POR_GAR_REAL_FIN
            query.append(", NULL");// POR_GAR_REAL_FIN
            //query.append(", ").append(st.nextToken());// SP_AJUST_GAR_REAL_FIN
            query.append(", NULL");// SP_AJUST_GAR_REAL_FIN
            //query.append(", ").append(st.nextToken());// EI_AJUST_GAR_REAL
            query.append(", NULL");// EI_AJUST_GAR_REAL
            //query.append(", ").append(st.nextToken());// POR_GAR_REAL_NO_FIN
            query.append(", NULL");// POR_GAR_REAL_NO_FIN
            //query.append(", ").append(st.nextToken());// SP_DER_COBRO
            query.append(", NULL");// SP_DER_COBRO
            //query.append(", ").append(st.nextToken());// SP_B_INMUEBLES
            query.append(", NULL");// SP_B_INMUEBLES
            //query.append(", ").append(st.nextToken());// SP_B_MUEBLES
            query.append(", NULL");// SP_B_MUEBLES
            //query.append(", ").append(st.nextToken());// SP_FID_GAR
            query.append(", NULL");// SP_FID_GAR
            //query.append(", ").append(st.nextToken());// SP_FID_ING_PROPIOS
            query.append(", NULL");// SP_FID_ING_PROPIOS
            //query.append(", ").append(st.nextToken());// SP_OTROS_REAL_NO_FIN
            query.append(", NULL");// SP_OTROS_REAL_NO_FIN
            //query.append(", ").append(st.nextToken());// SP_AJUST_GAR_REAL_NO_FIN
            query.append(", NULL");// SP_AJUST_GAR_REAL_NO_FIN
            //query.append(", ").append(st.nextToken());// POR_CUB_GAR_PER
            query.append(", NULL");// POR_CUB_GAR_PER
            //query.append(", ").append(st.nextToken());// POR_CUB_OBLIGADO
            query.append(", NULL");// POR_CUB_OBLIGADO
            //query.append(", ").append(st.nextToken());// MONTO_CUB_GP
            query.append(", NULL");// MONTO_CUB_GP
            //query.append(", ").append(st.nextToken());// POR_CUB_PYM
            query.append(", NULL");// POR_CUB_PYM
            //query.append(", ").append(st.nextToken());// POR_CUB_PP
            query.append(", NULL");// POR_CUB_PP
            //query.append(", ").append(st.nextToken());// MONTO_CUB_PYM
            query.append(", NULL");// MONTO_CUB_PYM
            //query.append(", ").append(st.nextToken());// MONTO_CUB_PP
            query.append(", NULL");// MONTO_CUB_PP
            //query.append(", ").append(st.nextToken());// RCSD
            query.append(", NULL");// RCSD
            //query.append(", ").append(st.nextToken());// PE_EYM
            query.append(", NULL");// PE_EYM
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), cadena));// FECHA_INFO
            query.append(", NULL");// FECHA_INFO
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // ID_PROYECTO
            query.append(", NULL");// ID_PROYECTO
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append(", NULL");
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append(", NULL");
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append(", NULL");
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), cadena));
            query.append(", NULL");
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append(", NULL");
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append(", NULL");
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append(", NULL");
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append(", NULL");
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append(", NULL");
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append(", NULL");
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append(", NULL");
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append(", NULL");
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append(", NULL");
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append(", NULL");
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append(", NULL");
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append(", NULL");
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append(", NULL");
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append(", NULL");
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append(", NULL");
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append(", NULL");
            //query.append(", ").append(validaCampos.validaStringNull(st.nextToken(), numero));
            query.append(", NULL");
      
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
        List<Map> listMap = new ArrayList<>();
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
            query.append("  AND ISC.TABLE_NAME = 'SICC_FALTANTES_CREDITO' ");
            query.append("  AND SCL.IND_ACTUALIZA = 1 ");
            query.append("  AND SCL.NUMERO_INSTITUCION = ").append(numeroInstitucion);
            query.append("  AND SCL.NUMERO_LAYOUT = ").append(noLayout);
            System.out.println(query);
            resultadoCampos = bd.executeQuery(query.toString());
            listCampos = sqlProperties.getColumnValue(resultadoCampos, listCampos);
            String campos;
            int i = 0;

            query.setLength(0);
            query.append("SELECT STL.NUMERO_CUENTA ");
            query.append("  ,STL.NUMERO_CONSECUTIVO ");
            while (i < listCampos.size()) {
                campos = listCampos.get(i).toString();
                campos = campos.substring(campos.indexOf("&#") + 2, campos.length());
                if (!campos.equals("MONTO_PAGO_EFECTIVO_COMISION") && !campos.equals("SDO_QUEBRANTO")) {
                    query.append("  ,STL.").append(campos);
                }
                i++;
            }
            query.append(" FROM SICC_CREDITO_LO AS STL ");
            query.append(" WHERE STL.NUMERO_CUENTA IS NOT NULL ");
            query.append("   AND EXISTS ( ");
            query.append("     SELECT SFT.NUMERO_CUENTA ");
            query.append("     FROM SICC_FALTANTES_CREDITO AS SFT ");
            query.append("     WHERE STL.NUMERO_INSTITUCION = SFT.NUMERO_INSTITUCION ");
            query.append("       AND STL.NUMERO_CUENTA = SFT.NUMERO_CUENTA ");
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
                    String numeroCuenta = stD.nextToken();
                    consecutivo = Integer.parseInt(stD.nextToken());
                    bd.setAutoCommit(false);
                    query.setLength(0);
                    query.append("INSERT INTO LOG_SICC_FALTANTES_CREDITO (");
                    query.append("  NUMERO_INSTITUCION ");
                    query.append("  ,NUMERO_CUENTA ");
                    query.append("  ,FECHA_MODIFICACION ");
                    query.append("  ,CLASIFICACION_CONTABLE");
                    query.append("  ,MOTIVO_CONDONACION");
                    query.append("  ,MONTO_DESCUENTOS");
                    query.append("  ,MONTO_OTROS");
                    query.append("  ,NOMBRE_FACTORADO");
                    query.append("  ,RFC_FACTORADO");
                    query.append("  ,PROG_GOB_FED");
                    query.append("  ,TIPO_CREDITO");
                    query.append("  ,CLAVE_PREVENCION");
                    query.append("  ,ESTIMAC_ADIC_RIESGOS_OPERA");
                    query.append("  ,ESTIMAC_ADIC_CRED_VENC");
                    query.append("  ,ESTIMAC_ADIC_REC_CNBV");
                    query.append("  ,PRODUCTO_COMERCIAL ");
                    query.append("  ,TIPO_LINEA ");
                    query.append("  ,GARANTIA_PERSONAL ");
                    query.append("  ,INFO_RECUPERACION_CREDITO ");
                    query.append("  ,POSICION ");
                    query.append("  ,EXPEDIENTE_COMPLETO ");
                    query.append("  ,OPERACION_FORMALIZADA ");
                    query.append("  ,METODOLOGIA ");
                    query.append("  ,REPORTADO_SIC ");
                    query.append("  ,PARTICIPACION_FEDERAL ");
                    query.append("  ,PORCENTAJE_PART_FEDERAL ");
                    query.append("  ,INTERES_REFINANCIADO ");
                    query.append("  ,EXPO_INCUMPLIMIENTO ");
                    query.append("  ,VAL_MERC_DERIV_CRED ");
                    query.append("  ,CONCURSO_MERCANTIL ");
                    query.append("  ,IND_EMPROBLEMADO ");
                    query.append("  ,CUMPLE_CRITERIO_CONT ");
                    query.append("  ,NUM_EMPRESTITOS_LOCAL ");
                    query.append("  ,NUM_EMPRESTITOS_SHCP ");
                    query.append("  ,COMISIONES_COBRADAS ");
                    query.append("  ,GASTOS_ORIGINACION_TASA ");
                    query.append("  ,COMISION_DISPOSICION_TASA ");
                    query.append("  ,COMISION_DISPOSICION_MONTO ");
                    query.append("  ,COMISION_ANUALIDAD ");
                    query.append("  ,MONTO_PAGO_EFECTIVO_COMISION ");
                    query.append("  ,TIPO_OPERACION ");
                    query.append("  ,TIPO_ALTA ");
                    query.append("  ,TIPO_BAJA ");
                    query.append("  ,TIPO_ALTA_MA ");
                    query.append("  ,TIPO_BAJA_MA ");
                    query.append("  ,FOLIO_CONSULTA_BURO ");
                    query.append("  ,MITIGANTE ");
                    query.append("  ,ES_PADRE ");
                    query.append("  ,VIGENCIA_INDEFINIDA ");
                    query.append("  ,CREDITO_SINDICADO ");
                    query.append("  ,INSTITUTO_FONDEA ");
                    query.append("  ,PORCENTAJE_GTIA_FONDO ");
                    query.append("  ,APOYO_BANCA_DESARROLLO ");
                    query.append("  ,SDO_QUITAS ");
                    query.append("  ,SDO_CASTIGOS ");
                    query.append("  ,SDO_CONDONACION ");
                    query.append("  ,SDO_QUEBRANTO ");
                    query.append("  ,SDO_DESCUENTOS ");
                    query.append("  ,SDO_DACION ");
                    query.append("  ,SDO_BONIFICACION ");
                    query.append("  ,DEUDOR_FACTORAJE ");
                    query.append("  ,RFC_DEUDOR_FACTORAJE ");
                    query.append(",DESTINO_CREDITO_MA");
                    
                    query.append(",OPER_DIF_TASA_DISP");
                    query.append(",OTROS_MONTOS_PAGADOS");
                    query.append(",ETAPA_DETERIORIO_NIFC16");
                    query.append(",CLASE_EFM");
                    query.append(",IND_DEUDA_ESTATAL");
                    query.append(",HIPOTESIS_PRESUNCION");
                    query.append(",SEGMENTO_NIFC16");
                    query.append(",NOMBRE_SISTEMA_NIFC16");
                    query.append(",INDICADOR_MI_NIFC16");
                    query.append(",CALIFICACION_NIFC16");
                    query.append(",RESERVAS_MI_C16_12MESES");
                    query.append(",RESERVAS_MI_C16_LIFETIME");
                    query.append(",SP_MI_C16");
                    query.append(",EI_MI_C16");
                    query.append(",FACT_CONV_CRED_NIFC16");
                    query.append(",TASA_DESC_NIFC16");
                    query.append(",TASA_INTERES_NIFC16");
                    query.append(",TASA_PREPAGO_NIFC16");
                    query.append(",PLAZO_LIFETIME_NIFC16");
                    query.append(",PLAZO_ORIGINAL_NIFC16");
                    query.append(",PLAZO_REMANENTE_NIFC16");
                    query.append(",HIPOTESIS_PRESUNCION_NIFC16");
                    query.append(",SEGMENTO_MI");
                    query.append(",NOMBRE_SISTEMA_MI");
                    query.append(",IND_ENFOQUE_MI");
                    query.append(",INDICADOR_MI");
                    query.append(",CALIFICACION_MI");
                    query.append(",SP_MI");
                    query.append(",EI_MI");
                    query.append(",VENCIMIENTO_MI");
                    query.append(",CORRELACION_MI");
                    query.append(",POND_REQ_CAP_MI");
                    query.append(",REQ_CAP_MI");
                    query.append(",PORCENTAJE_PISO_MI");
                    query.append(",VALUACION_DERIVADO_CRED");
                    query.append(",MON_VALUACION_DERIVADO");
                    query.append(",ID_PPP");
                    query.append(",ID_PYM");
                    query.append(",FUENTE_PAGO");
                    query.append(",SALDO_DEUDA");
                    query.append(",FONDO_RESERVA_EYM");
                    query.append(",AGENCIA_CAL_EYM");
                    query.append(",CALIFICACION_EYM");
                    query.append(",CLAS_CONTABLE_417");
                    query.append(",PE_CE");
                    query.append(",RES_12MESES");
                    query.append(",RES_GAR_12MESES");
                    query.append(",RES_CLIENTE_12MESES");
                    query.append(",RES_LIFETIME");
                    query.append(",RES_TOTALES");
                    query.append(",RES_CONSTITUIDAS");
                    query.append(",RES_DESCONSTITUIDAS");
                    query.append(",PI_TOTAL");
                    query.append(",PI_GARANTE");
                    query.append(",PI_ACREDITADO");
                    query.append(",SP_TOTAL");
                    query.append(",SP_GARANTE");
                    query.append(",SP_ACREDITADO");
                    query.append(",EI_TOTAL");
                    query.append(",EI_GARANTE");
                    query.append(",EI_ACREDITADO");
                    query.append(",PLAZO_ORIGINAL");
                    query.append(",PLAZO_REMANENTE");
                    query.append(",SUST_PI");
                    query.append(",MESES_PI_100");
                    query.append(",GRADO_RIESGO");
                    query.append(",RESERVAS_ADIC_CONSTITUIDAS");
                    query.append(",RESERVAS_ADIC_DESCONSTITUIDAS");
                    query.append(",RESERVAS_MI");
                    query.append(",FACTOR_CONV_RIESGO");
                    query.append(",EXP_AJUSTADA_MIT");
                    query.append(",EXP_NETA_RES");
                    query.append(",POND_RIESGO");
                    query.append(",REQ_CAP_CRED");
                    query.append(",POR_NO_CUBIERTO");
                    query.append(",SP_NO_CUBIERTO");
                    query.append(",POR_GAR_REAL_FIN");
                    query.append(",SP_AJUST_GAR_REAL_FIN");
                    query.append(",EI_AJUST_GAR_REAL");
                    query.append(",POR_GAR_REAL_NO_FIN");
                    query.append(",SP_DER_COBRO");
                    query.append(",SP_B_INMUEBLES");
                    query.append(",SP_B_MUEBLES");
                    query.append(",SP_FID_GAR");
                    query.append(",SP_FID_ING_PROPIOS");
                    query.append(",SP_OTROS_REAL_NO_FIN");
                    query.append(",SP_AJUST_GAR_REAL_NO_FIN");
                    query.append(",POR_CUB_GAR_PER");
                    query.append(",POR_CUB_OBLIGADO");
                    query.append(",MONTO_CUB_GP");
                    query.append(",POR_CUB_PYM");
                    query.append(",POR_CUB_PP");
                    query.append(",MONTO_CUB_PYM");
                    query.append(",MONTO_CUB_PP");
                    query.append(",RCSD");
                    query.append(",PE_EYM");
                    query.append(",FECHA_INFO");
                    query.append(",ID_PROYECTO");
                    
                    query.append(" ,RC_CALIFICACION");
                    query.append(" ,VALOR_MITIGANTE");
                    query.append(" ,MONEDA_MITIGANTE");
                    query.append(" ,CODIGOCREDITOREESTRUCTURA");
                    query.append(" ,TAMANO_ACREDITADO");
                    query.append(" ,SALDO_BAJA");
                    query.append(" ,MONTO_PAGADO_BAJA");
                    query.append(" ,EI_EXPUESTA");
                    query.append(" ,NUM_GAR_REAL_FIN");
                    query.append(" ,HFX");
                    query.append(" ,VAL_CONT_GAR_REAL_FIN");
                    query.append(" ,NUM_GAR_REAL_NO_FIN");
                    query.append(" ,GTIA_DER_COBRO");
                    query.append(" ,GTIA_BIEN_INMUEBLE");
                    query.append(" ,GTIA_BIEN_MUEBLE");
                    query.append(" ,GTIA_FIDEICOM_EYM");
                    query.append(" ,GTIA_FIDEICOM_ING_PROP");
                    query.append(" ,GTIA_OTROS_REAL_NO_FIN");
                    query.append(" ,NUM_GAR_PERS");
                    query.append(" ,SUST_PI_2");
                    query.append(" ,PI_ASEG");

                    query.append("  ,STATUS ");
                    query.append("  ,ID_USUARIO_MODIFICACION ");
                    query.append("  ,MAC_ADDRESS_MODIFICACION ");
                    query.append("  ) ");
                    query.append("SELECT NUMERO_INSTITUCION ");
                    query.append("  ,NUMERO_CUENTA ");
                    query.append("  ,'").append(fechaModificacion).append("' ");
                    query.append("  ,CLASIFICACION_CONTABLE");
                    query.append("  ,MOTIVO_CONDONACION");
                    query.append("  ,MONTO_DESCUENTOS");
                    query.append("  ,MONTO_OTROS");
                    query.append("  ,NOMBRE_FACTORADO");
                    query.append("  ,RFC_FACTORADO");
                    query.append("  ,PROG_GOB_FED");
                    query.append("  ,TIPO_CREDITO");
                    query.append("  ,CLAVE_PREVENCION");
                    query.append("  ,ESTIMAC_ADIC_RIESGOS_OPERA");
                    query.append("  ,ESTIMAC_ADIC_CRED_VENC");
                    query.append("  ,ESTIMAC_ADIC_REC_CNBV");
                    query.append("  ,PRODUCTO_COMERCIAL ");
                    query.append("  ,TIPO_LINEA ");
                    query.append("  ,GARANTIA_PERSONAL ");
                    query.append("  ,INFO_RECUPERACION_CREDITO ");
                    query.append("  ,POSICION ");
                    query.append("  ,EXPEDIENTE_COMPLETO ");
                    query.append("  ,OPERACION_FORMALIZADA ");
                    query.append("  ,METODOLOGIA ");
                    query.append("  ,REPORTADO_SIC ");
                    query.append("  ,PARTICIPACION_FEDERAL ");
                    query.append("  ,PORCENTAJE_PART_FEDERAL ");
                    query.append("  ,INTERES_REFINANCIADO ");
                    query.append("  ,EXPO_INCUMPLIMIENTO ");
                    query.append("  ,VAL_MERC_DERIV_CRED ");
                    query.append("  ,CONCURSO_MERCANTIL ");
                    query.append("  ,IND_EMPROBLEMADO ");
                    query.append("  ,CUMPLE_CRITERIO_CONT ");
                    query.append("  ,NUM_EMPRESTITOS_LOCAL ");
                    query.append("  ,NUM_EMPRESTITOS_SHCP ");
                    query.append("  ,COMISIONES_COBRADAS ");
                    query.append("  ,GASTOS_ORIGINACION_TASA ");
                    query.append("  ,COMISION_DISPOSICION_TASA ");
                    query.append("  ,COMISION_DISPOSICION_MONTO ");
                    query.append("  ,COMISION_ANUALIDAD ");
                    query.append("  ,MONTO_PAGO_EFECTIVO_COMISION ");
                    query.append("  ,TIPO_OPERACION ");
                    query.append("  ,TIPO_ALTA ");
                    query.append("  ,TIPO_BAJA ");
                    query.append("  ,TIPO_ALTA_MA ");
                    query.append("  ,TIPO_BAJA_MA ");
                    query.append("  ,FOLIO_CONSULTA_BURO ");
                    query.append("  ,MITIGANTE ");
                    query.append("  ,ES_PADRE ");
                    query.append("  ,VIGENCIA_INDEFINIDA ");
                    query.append("  ,CREDITO_SINDICADO ");
                    query.append("  ,INSTITUTO_FONDEA ");
                    query.append("  ,PORCENTAJE_GTIA_FONDO ");
                    query.append("  ,APOYO_BANCA_DESARROLLO ");
                    query.append("  ,SDO_QUITAS ");
                    query.append("  ,SDO_CASTIGOS ");
                    query.append("  ,SDO_CONDONACION ");
                    query.append("  ,SDO_QUEBRANTO ");
                    query.append("  ,SDO_DESCUENTOS ");
                    query.append("  ,SDO_DACION ");
                    query.append("  ,SDO_BONIFICACION ");
                    query.append("  ,DEUDOR_FACTORAJE ");
                    query.append("  ,RFC_DEUDOR_FACTORAJE ");
                    query.append("  ,DESTINO_CREDITO_MA");
                    
                    query.append("  ,OPER_DIF_TASA_DISP");
                    query.append("  ,OTROS_MONTOS_PAGADOS");
                    query.append("  ,ETAPA_DETERIORIO_NIFC16");
                    query.append("  ,CLASE_EFM");
                    query.append("  ,IND_DEUDA_ESTATAL");
                    query.append("  ,HIPOTESIS_PRESUNCION");
                    query.append("  ,SEGMENTO_NIFC16");
                    query.append("  ,NOMBRE_SISTEMA_NIFC16");
                    query.append("  ,INDICADOR_MI_NIFC16");
                    query.append("  ,CALIFICACION_NIFC16");
                    query.append("  ,RESERVAS_MI_C16_12MESES");
                    query.append("  ,RESERVAS_MI_C16_LIFETIME");
                    query.append("  ,SP_MI_C16");
                    query.append("  ,EI_MI_C16");
                    query.append("  ,FACT_CONV_CRED_NIFC16");
                    query.append("  ,TASA_DESC_NIFC16");
                    query.append("  ,TASA_INTERES_NIFC16");
                    query.append("  ,TASA_PREPAGO_NIFC16");
                    query.append("  ,PLAZO_LIFETIME_NIFC16");
                    query.append("  ,PLAZO_ORIGINAL_NIFC16");
                    query.append("  ,PLAZO_REMANENTE_NIFC16");
                    query.append("  ,HIPOTESIS_PRESUNCION_NIFC16");
                    query.append("  ,SEGMENTO_MI");
                    query.append("  ,NOMBRE_SISTEMA_MI");
                    query.append("  ,IND_ENFOQUE_MI");
                    query.append("  ,INDICADOR_MI");
                    query.append("  ,CALIFICACION_MI");
                    query.append("  ,SP_MI");
                    query.append("  ,EI_MI");
                    query.append("  ,VENCIMIENTO_MI");
                    query.append("  ,CORRELACION_MI");
                    query.append("  ,POND_REQ_CAP_MI");
                    query.append("  ,REQ_CAP_MI");
                    query.append("  ,PORCENTAJE_PISO_MI");
                    query.append("  ,VALUACION_DERIVADO_CRED");
                    query.append("  ,MON_VALUACION_DERIVADO");
                    query.append("  ,ID_PPP");
                    query.append("  ,ID_PYM");
                    query.append("  ,FUENTE_PAGO");
                    query.append("  ,SALDO_DEUDA");
                    query.append("  ,FONDO_RESERVA_EYM");
                    query.append("  ,AGENCIA_CAL_EYM");
                    query.append("  ,CALIFICACION_EYM");
                    query.append("  ,CLAS_CONTABLE_417");
                    query.append("  ,PE_CE");
                    query.append("  ,RES_12MESES");
                    query.append("  ,RES_GAR_12MESES");
                    query.append("  ,RES_CLIENTE_12MESES");
                    query.append("  ,RES_LIFETIME");
                    query.append("  ,RES_TOTALES");
                    query.append("  ,RES_CONSTITUIDAS");
                    query.append("  ,RES_DESCONSTITUIDAS");
                    query.append("  ,PI_TOTAL");
                    query.append("  ,PI_GARANTE");
                    query.append("  ,PI_ACREDITADO");
                    query.append("  ,SP_TOTAL");
                    query.append("  ,SP_GARANTE");
                    query.append("  ,SP_ACREDITADO");
                    query.append("  ,EI_TOTAL");
                    query.append("  ,EI_GARANTE");
                    query.append("  ,EI_ACREDITADO");
                    query.append("  ,PLAZO_ORIGINAL");
                    query.append("  ,PLAZO_REMANENTE");
                    query.append("  ,SUST_PI");
                    query.append("  ,MESES_PI_100");
                    query.append("  ,GRADO_RIESGO");
                    query.append("  ,RESERVAS_ADIC_CONSTITUIDAS");
                    query.append("  ,RESERVAS_ADIC_DESCONSTITUIDAS");
                    query.append("  ,RESERVAS_MI");
                    query.append("  ,FACTOR_CONV_RIESGO");
                    query.append("  ,EXP_AJUSTADA_MIT");
                    query.append("  ,EXP_NETA_RES");
                    query.append("  ,POND_RIESGO");
                    query.append("  ,REQ_CAP_CRED");
                    query.append("  ,POR_NO_CUBIERTO");
                    query.append("  ,SP_NO_CUBIERTO");
                    query.append("  ,POR_GAR_REAL_FIN");
                    query.append("  ,SP_AJUST_GAR_REAL_FIN");
                    query.append("  ,EI_AJUST_GAR_REAL");
                    query.append("  ,POR_GAR_REAL_NO_FIN");
                    query.append("  ,SP_DER_COBRO");
                    query.append("  ,SP_B_INMUEBLES");
                    query.append("  ,SP_B_MUEBLES");
                    query.append("  ,SP_FID_GAR");
                    query.append("  ,SP_FID_ING_PROPIOS");
                    query.append("  ,SP_OTROS_REAL_NO_FIN");
                    query.append("  ,SP_AJUST_GAR_REAL_NO_FIN");
                    query.append("  ,POR_CUB_GAR_PER");
                    query.append("  ,POR_CUB_OBLIGADO");
                    query.append("  ,MONTO_CUB_GP");
                    query.append("  ,POR_CUB_PYM");
                    query.append("  ,POR_CUB_PP");
                    query.append("  ,MONTO_CUB_PYM");
                    query.append("  ,MONTO_CUB_PP");
                    query.append("  ,RCSD");
                    query.append("  ,PE_EYM");
                    query.append("  ,FECHA_INFO");
                    query.append("  ,ID_PROYECTO");
                    
                    query.append(" ,RC_CALIFICACION");
                    query.append(" ,VALOR_MITIGANTE");
                    query.append(" ,MONEDA_MITIGANTE");
                    query.append(" ,CODIGOCREDITOREESTRUCTURA");
                    query.append(" ,TAMANO_ACREDITADO");
                    query.append(" ,SALDO_BAJA");
                    query.append(" ,MONTO_PAGADO_BAJA");
                    query.append(" ,EI_EXPUESTA");
                    query.append(" ,NUM_GAR_REAL_FIN");
                    query.append(" ,HFX");
                    query.append(" ,VAL_CONT_GAR_REAL_FIN");
                    query.append(" ,NUM_GAR_REAL_NO_FIN");
                    query.append(" ,GTIA_DER_COBRO");
                    query.append(" ,GTIA_BIEN_INMUEBLE");
                    query.append(" ,GTIA_BIEN_MUEBLE");
                    query.append(" ,GTIA_FIDEICOM_EYM");
                    query.append(" ,GTIA_FIDEICOM_ING_PROP");
                    query.append(" ,GTIA_OTROS_REAL_NO_FIN");
                    query.append(" ,NUM_GAR_PERS");
                    query.append(" ,SUST_PI_2");
                    query.append(" ,PI_ASEG");
                    
                    query.append("  ,STATUS ");
                    query.append("  ,'").append(idUsuario).append("' ");
                    query.append("  ,'").append(macAddress).append("' ");
                    query.append("FROM SICC_FALTANTES_CREDITO ");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  AND NUMERO_CUENTA = ").append(numeroCuenta);
                    System.out.println(query);
                    bd.executeUpdate(query.toString());

                    indAlta = getvalidaAltaBaja(numeroInstitucion, Long.parseLong(numeroCuenta),(short) 1, bd);
                    indBaja = getvalidaAltaBaja(numeroInstitucion, Long.parseLong(numeroCuenta),(short) 2, bd);
                    
                    query.setLength(0);
                    query.append("UPDATE SICC_FALTANTES_CREDITO ");
                    query.append("SET ");
                    query.append(updateCommon.armaUpdateCredito(listCampos, stD, indAlta, indBaja));
                    query.append("  ,FECHA_MODIFICACION = '").append(fechaModificacion).append("'");
                    query.append("  ,ID_USUARIO_MODIFICACION = '").append(idUsuario).append("'");
                    query.append("  ,MAC_ADDRESS_MODIFICACION = '").append(macAddress).append("'");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  AND NUMERO_CUENTA = ").append(numeroCuenta);
                    System.out.println(query);
                    bd.executeUpdate(query.toString());

                    indAlta = 0;
                    indBaja = 0;
                    
                    bd.commit();
                } catch (SQLException e) {
                	e.printStackTrace();
                    bd.rollback();
                    query.setLength(0);
                    query.append("UPDATE SICC_CREDITO_LO ");
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
    
    public byte getvalidaAltaBaja(short numeroInstitucion, long numeroCuenta, short indicador, JDBCConnectionPool bd) throws Exception {
    	List<String> listDatos = new ArrayList<String>();
    	byte actualiza = 0;
    	try {
    		StringBuffer query = new StringBuffer();
    		
    		query.append(" "); 
    		query.append("SELECT NUMERO_CUENTA "); 
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
    		query.append("FROM CRE_CUENTAS A "); 
    		query.append("INNER JOIN GRAL_FECHAS_SISTEMA B ON A.NUMERO_INSTITUCION = B.NUMERO_INSTITUCION "); 
    		query.append(" AND B.CLAVE_SISTEMA = 'CREDITO' ");
    		query.append("INNER JOIN SICC_PARAMETROS C ON A.NUMERO_INSTITUCION = C.NUMERO_INSTITUCION "); 
    		query.append("WHERE A.NUMERO_INSTITUCION = ").append(numeroInstitucion);
    		query.append(" AND NUMERO_CUENTA = ").append(numeroCuenta);
    		
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
}

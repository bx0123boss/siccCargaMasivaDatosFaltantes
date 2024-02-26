package OrionSFI.core.siccCargaMasivaDatosFaltantes;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import OrionSFI.core.commons.InspectorDatos;
import OrionSFI.core.commons.JDBCConnectionPool;
import OrionSFI.core.commons.MensajesSistema;
import OrionSFI.core.commons.SQLProperties;

public class SICC_ANEXO19 {

	
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
        //expRegular.append("(\\d{1,5}+\\|)");                                                             	//NUMERO_CONSECUTIVO
        expRegular.append("(\\d{1,15}+\\|)");                                     	                       	//NUMERO_CUENTA
        expRegular.append("([a-zA-Z0-9-_ ]{1,250}+\\|)");          					                       	//NOMBRE_PROYECTO
        expRegular.append("([a-zA-Z0-9-_ ]{0,250}+\\|)");          					                       	//DESCRIPCION_PROYECTO
        expRegular.append("(\\d{1,21}+\\|)");                                     	                       	//SOBRE_COSTO
        expRegular.append("(\\d{1,21}+\\|)");                                     	                       	//MONTO_CUBIERTO_TERCEROS
        expRegular.append("(\\d{1,21}+\\|)");                                     	                       	//MESES_CONTEMPLADOS
        expRegular.append("(\\d{0,21}+\\|)");                                     	                       	//MESES_ADICIONALES
        expRegular.append("(\\d{1,21}+\\|)");                                     	                       	//VP_TOTAL_DEF_SUPER
        expRegular.append("(\\d{1,21}+\\|)");                                     	                       	//UTILIDAD_PERDIDA_ACUMULADA
        expRegular.append("(\\d{1,21}+\\|)");                                     	                       	//ETAPA
        expRegular.append("((0?[1-9]|[12][0-9]|3[01])?(/|-)?(0?[1-9]|1[012])?(/|-)?((19|20)\\d\\d)?+\\|)");	//FECHA_INICIO_OERACION
        //expRegular.append("(\\d{1,10}+[\\.]?+\\d{0,6}+\\|)");                     							//TASA_DESCUENTO_VP
        expRegular.append("(\\d{1,10}|\\d{1,10}+\\.+\\d{0,6})"); 											//TASA_DESCUENTO_VP
        
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

        if (iData.isStringNULL(cad[0])) {
        	if (cad[0].equalsIgnoreCase("NULL")){
                errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NUMERO_CUENTA", 15, noLayout, consecutivo, (byte) 1)).append(pipe);
            	}else{
                    errRegistro.append(validaCampos.getCampoNumerico(cad[0], "NUMERO_CUENTA", 15, noLayout, consecutivo, (byte) 1)).append(pipe);
            	}
        } else {
            errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NUMERO_CUENTA", 15, noLayout, consecutivo, (byte) 1)).append(pipe);
        }
        
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Nombre de Proyecto", 250, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "descripción de proyecto", 250, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "sobre costo de la obra", 21, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Monto cubierto por terceros para el proyecto", 21, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Meses Contemplados para el proyecto", 21, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Meses adicionales para el proyecto", 21, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "valor presente Total (Deficit/Superavit)", 21, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Utilidad/Perdida acumulada de Flujo", 21, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "clave de Etapa", 1, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "fecha de inicio de operación del proyecto", 10, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "tasa de descuento para valor presente", 16, noLayout, consecutivo, seisDecimales, (byte) 1)).append(pipe);
        
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "TIPO_PROYECTO", 5, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "POR_EXPOS_29_INST_BAN", 16, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "VP_FEGP", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "ANALISIS_ESTRES", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "ESTRUCTURA_FINANCIERA", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "RIES_POL_ENT_REG", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "RIESGO_EVENTOS_FUERZA_MAY", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "ADQ_APOYOS_APROBAC", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "CUMPLIMIENTO_CONTRATOS", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "RIESGO_CONSTRUCCION", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TIPO_CONTRATO_CONSTRUCCION", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "RIESGO_OPERATIVO", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "RIESGO_SUMINISTRO", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "PRENDA_ACTIVOS", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "CONTROL_INST_FLUJO_EFECTIVO", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "FONDOS_RESERVA", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "HISTORIAL_PATROCINADOR", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "COBERTURA_SEGUROS", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "ACTIVO_BRGR", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "ACTIVO_FA", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "ACTIVO_FC", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "ACTIVO_FP_FP1", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "INGRESOS_PROPIOS_PROY", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "GASTOS_TOTALES_PROY", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "IMPACTO_FISCAL_PROY", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PAGO_DEDUDA_12MESES", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "FLUJO_EFECTIVO_ADICIONAL", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_RAZONES_FINANCIERAS", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_ANALISIS_ESTRES", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_ESTRUCTURA_FINANCIERA", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_RIES_POL_ENT_REG", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_RIESGO_EVENTOS_FUERZA_MAY", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_ADQ_APOYOS_APROBAC", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_CUMPLIMIENTO_CONTRATOS", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_RIESGO_CONSTRUCCION", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_TIPO_CONTRATO_CONSTRUCCION", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_RIESGO_OPERATIVO", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_RIESGO_SUMINISTRO", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_PRENDA_ACTIVOS", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_CONTROL_INST_FLUJO_EFECTIVO", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_FONDOS_RESERVA", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_HISTORIAL_PATROCINADOR", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_COBERTURA_SEGUROS", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_ETAPA_PROYECTO", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "P_ACTIVO_BRGR", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "FLUJ_EF_GEN_PROY", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "GARANTIAS", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "VAL_DESC_ACT_SUBY", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "VAL_ACT_SUBY", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "NIVEL_C", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "VAL_GAR_REAL_NO_FIN", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "VAL_DESC_GAR_REAL_NO_FIN", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "VAL_GAR_REAL_FIN", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "VAL_DESC_GAR_REAL_FIN", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "FECHA_INFO", 10, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "ID_PROYECTO", 22, noLayout, consecutivo, (byte) 1)).append(pipe);
        
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Días Atraso", 10, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Porcentaje Pérdida Esperada", 16, noLayout, consecutivo,seisDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Puntaje Crediticio Total", 6, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Puntaje Crediticio Cuantitativo", 6, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Puntaje Crediticio Cualitativo", 6, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Puntaje Porcentaje Exposiciones", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Puntaje Porcentaje Cobertura", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Puntaje Días Atraso", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Porcentaje Cobertura", 16, noLayout, consecutivo,seisDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Factor Conversión Riesgo", 16, noLayout, consecutivo,seisDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Exposición Ajustada Mitigantes", 23, noLayout, consecutivo,dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Exposición Neta Reservas", 23, noLayout, consecutivo,dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Ponderador Riesgo", 10, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Requerimiento Capital por Crédito", 23, noLayout, consecutivo,dosDecimales, (byte) 1)).append(pipe);
        

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
            query.append("INSERT INTO SICC_ANEXO19_LO(");
            query.append(" NUMERO_INSTITUCION "); 
            query.append(" ,NOMBRE_ARCHIVO "); 
            query.append(" ,FOLIO_LOTE "); 
            query.append(" ,NUMERO_LAYOUT "); 
            query.append(" ,NUMERO_CONSECUTIVO "); 
            query.append(" ,NUMERO_CUENTA "); 
            query.append(" ,NOMBRE_PROYECTO "); 
            query.append(" ,DESCRIPCION_PROYECTO "); 
            query.append(" ,SOBRE_COSTO "); 
            query.append(" ,MONTO_CUBIERTO_TERCEROS "); 
            query.append(" ,MESES_CONTEMPLADOS "); 
            query.append(" ,MESES_ADICIONALES "); 
            query.append(" ,VP_TOTAL_DEF_SUPER "); 
            query.append(" ,UTILIDAD_PERDIDA_ACUMULADA "); 
            query.append(" ,ETAPA "); 
            query.append(" ,FECHA_INICIO_OPERACION "); 
            query.append(" ,TASA_DESCUENTO_VP ");
            
            query.append(" ,TIPO_PROYECTO ");
            query.append(" ,POR_EXPOS_29_INST_BAN ");
            query.append(" ,VP_FEGP ");
            query.append(" ,ANALISIS_ESTRES ");
            query.append(" ,ESTRUCTURA_FINANCIERA ");
            query.append(" ,RIES_POL_ENT_REG ");
            query.append(" ,RIESGO_EVENTOS_FUERZA_MAY ");
            query.append(" ,ADQ_APOYOS_APROBAC ");
            query.append(" ,CUMPLIMIENTO_CONTRATOS ");
            query.append(" ,RIESGO_CONSTRUCCION ");
            query.append(" ,TIPO_CONTRATO_CONSTRUCCION ");
            query.append(" ,RIESGO_OPERATIVO ");
            query.append(" ,RIESGO_SUMINISTRO ");
            query.append(" ,PRENDA_ACTIVOS ");
            query.append(" ,CONTROL_INST_FLUJO_EFECTIVO ");
            query.append(" ,FONDOS_RESERVA ");
            query.append(" ,HISTORIAL_PATROCINADOR ");
            query.append(" ,COBERTURA_SEGUROS ");
            query.append(" ,ACTIVO_BRGR ");
            query.append(" ,ACTIVO_FA ");
            query.append(" ,ACTIVO_FC ");
            query.append(" ,ACTIVO_FP_FP1 ");
            query.append(" ,INGRESOS_PROPIOS_PROY ");
            query.append(" ,GASTOS_TOTALES_PROY ");
            query.append(" ,IMPACTO_FISCAL_PROY ");
            query.append(" ,PAGO_DEDUDA_12MESES ");
            query.append(" ,FLUJO_EFECTIVO_ADICIONAL ");
            query.append(" ,P_RAZONES_FINANCIERAS ");
            query.append(" ,P_ANALISIS_ESTRES ");
            query.append(" ,P_ESTRUCTURA_FINANCIERA ");
            query.append(" ,P_RIES_POL_ENT_REG ");
            query.append(" ,P_RIESGO_EVENTOS_FUERZA_MAY ");
            query.append(" ,P_ADQ_APOYOS_APROBAC ");
            query.append(" ,P_CUMPLIMIENTO_CONTRATOS ");
            query.append(" ,P_RIESGO_CONSTRUCCION ");
            query.append(" ,P_TIPO_CONTRATO_CONSTRUCCION ");
            query.append(" ,P_RIESGO_OPERATIVO ");
            query.append(" ,P_RIESGO_SUMINISTRO ");
            query.append(" ,P_PRENDA_ACTIVOS ");
            query.append(" ,P_CONTROL_INST_FLUJO_EFECTIVO ");
            query.append(" ,P_FONDOS_RESERVA ");
            query.append(" ,P_HISTORIAL_PATROCINADOR ");
            query.append(" ,P_COBERTURA_SEGUROS ");
            query.append(" ,P_ETAPA_PROYECTO ");
            query.append(" ,P_ACTIVO_BRGR ");
            query.append(" ,FLUJ_EF_GEN_PROY ");
            query.append(" ,GARANTIAS ");
            query.append(" ,VAL_DESC_ACT_SUBY ");
            query.append(" ,VAL_ACT_SUBY ");
            query.append(" ,NIVEL_C ");
            query.append(" ,VAL_GAR_REAL_NO_FIN ");
            query.append(" ,VAL_DESC_GAR_REAL_NO_FIN ");
            query.append(" ,VAL_GAR_REAL_FIN ");
            query.append(" ,VAL_DESC_GAR_REAL_FIN ");
            query.append(" ,FECHA_INFO ");
            query.append(" ,ID_PROYECTO ");
            
            query.append(" ,DIAS_ATR_INST ");
            query.append(" ,PORC_PE ");
            query.append(" ,PUNT_TOT_PI_A19 ");
            query.append(" ,PUNT_CUANT_PI_A19 ");
            query.append(" ,PUNT_CUALI_PI_A19 ");
            query.append(" ,PUNT_PORC_EXP_29DIAS ");
            query.append(" ,PUNT_PORC_COB_CRED ");
            query.append(" ,PUNT_DIAS_ATR_INST ");
            query.append(" ,PORC_COB_CRED ");
            query.append(" ,FACT_CONV_RC ");
            query.append(" ,EI_AJUST_MIT ");
            query.append(" ,EI_NETA_RES ");
            query.append(" ,POND_RIESGO ");
            query.append(" ,REQ_ICAP ");

            query.append(" ,STATUS_CARGA "); 
            query.append(" ,STATUS_ALTA "); 
            query.append(" ,STATUS "); 
            query.append(" ,FECHA_ALTA "); 
            query.append(" ,ID_USUARIO_ALTA "); 
            query.append(" ,MAC_ADDRESS_ALTA "); 
            query.append(" ,FECHA_MODIFICACION "); 
            query.append(" ,ID_USUARIO_MODIFICACION "); 
            query.append(" ,MAC_ADDRESS_MODIFICACION) "); 
            query.append("VALUES(");
            query.append("  ").append(numeroInstitucion);			                  //NUMERO_INSTITUCION
            query.append("  ,'").append(nombreArchivo).append("'");                               //NOMBRE_ARCHIVO
            query.append("  ,'").append(folioLote).append("'");                                   //FOLIO_LOTE
            query.append("  ,").append(noLayout);                                                 //NUMERO_LAYOUT
            query.append("  ,").append(st.nextToken());                                           //NUMERO_CONSECUTIVO
            query.append("  ,").append(st.nextToken());                                           //NUMERO_CUENTA
            String NOMBRE_PROYECTO = st.nextToken();
            query.append("  ,'").append(iDat.isStringNULLExt(NOMBRE_PROYECTO)?"&#":NOMBRE_PROYECTO).append("'"); //NOMBRE_PROYECTO
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena));  //DESCRIPCION_PROYECTO
            query.append("  ,").append(st.nextToken()); 										  //SOBRE_COSTO
            query.append("  ,").append(st.nextToken()); 										  //MONTO_CUBIERTO_TERCEROS
            query.append("  ,").append(st.nextToken());                                           //MESES_CONTEMPLADOS
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //MESES_ADICIONALES
            query.append("  ,").append(st.nextToken()); 										  //VP_TOTAL_DEF_SUPER
            query.append("  ,").append(st.nextToken()); 										  //UTILIDAD_PERDIDA_ACUMULADA
            query.append("  ,").append(st.nextToken()); 										  //ETAPA
            query.append("  ,'").append(st.nextToken()).append("'"); 							  //FECHA_INICIO_OERACION
            query.append("  ,").append(st.nextToken());                                           //TASA_DESCUENTO_VP
            
            query.append("  ,'").append(st.nextToken()).append("'");  //TIPO_PROYECTO
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), numero)); //POR_EXPOS_29_INST_BAN
            query.append("  ,").append(st.nextToken()); //VP_FEGP
            query.append("  ,").append(st.nextToken()); //ANALISIS_ESTRES
            query.append("  ,").append(st.nextToken()); //ESTRUCTURA_FINANCIERA
            query.append("  ,").append(st.nextToken()); //RIES_POL_ENT_REG
            query.append("  ,").append(st.nextToken()); //RIESGO_EVENTOS_FUERZA_MAY
            query.append("  ,").append(st.nextToken()); //ADQ_APOYOS_APROBAC
            query.append("  ,").append(st.nextToken()); //CUMPLIMIENTO_CONTRATOS
            query.append("  ,").append(st.nextToken()); //RIESGO_CONSTRUCCION
            query.append("  ,").append(st.nextToken()); //TIPO_CONTRATO_CONSTRUCCION
            query.append("  ,").append(st.nextToken()); //RIESGO_OPERATIVO
            query.append("  ,").append(st.nextToken()); //RIESGO_SUMINISTRO
            query.append("  ,").append(st.nextToken()); //PRENDA_ACTIVOS
            query.append("  ,").append(st.nextToken()); //CONTROL_INST_FLUJO_EFECTIVO
            query.append("  ,").append(st.nextToken()); //FONDOS_RESERVA
            query.append("  ,").append(st.nextToken()); //HISTORIAL_PATROCINADOR
            query.append("  ,").append(st.nextToken()); //COBERTURA_SEGUROS
            query.append("  ,").append(st.nextToken()); //ACTIVO_BRGR
            query.append("  ,").append(st.nextToken()); //ACTIVO_FA
            query.append("  ,").append(st.nextToken()); //ACTIVO_FC
            query.append("  ,").append(st.nextToken()); //ACTIVO_FP_FP1
            query.append("  ,").append(st.nextToken()); //INGRESOS_PROPIOS_PROY
            query.append("  ,").append(st.nextToken()); //GASTOS_TOTALES_PROY
            query.append("  ,").append(st.nextToken()); //IMPACTO_FISCAL_PROY
            query.append("  ,").append(st.nextToken()); //PAGO_DEDUDA_12MESES
            query.append("  ,").append(st.nextToken()); //FLUJO_EFECTIVO_ADICIONAL
            query.append("  ,").append(st.nextToken()); //P_RAZONES_FINANCIERAS
            query.append("  ,").append(st.nextToken()); //P_ANALISIS_ESTRES
            query.append("  ,").append(st.nextToken()); //P_ESTRUCTURA_FINANCIERA
            query.append("  ,").append(st.nextToken()); //P_RIES_POL_ENT_REG
            query.append("  ,").append(st.nextToken()); //P_RIESGO_EVENTOS_FUERZA_MAY
            query.append("  ,").append(st.nextToken()); //P_ADQ_APOYOS_APROBAC
            query.append("  ,").append(st.nextToken()); //P_CUMPLIMIENTO_CONTRATOS
            query.append("  ,").append(st.nextToken()); //P_RIESGO_CONSTRUCCION
            query.append("  ,").append(st.nextToken()); //P_TIPO_CONTRATO_CONSTRUCCION
            query.append("  ,").append(st.nextToken()); //P_RIESGO_OPERATIVO
            query.append("  ,").append(st.nextToken()); //P_RIESGO_SUMINISTRO
            query.append("  ,").append(st.nextToken()); //P_PRENDA_ACTIVOS
            query.append("  ,").append(st.nextToken()); //P_CONTROL_INST_FLUJO_EFECTIVO
            query.append("  ,").append(st.nextToken()); //P_FONDOS_RESERVA
            query.append("  ,").append(st.nextToken()); //P_HISTORIAL_PATROCINADOR
            query.append("  ,").append(st.nextToken()); //P_COBERTURA_SEGUROS
            query.append("  ,").append(st.nextToken()); //P_ETAPA_PROYECTO
            query.append("  ,").append(st.nextToken()); //P_ACTIVO_BRGR
            query.append("  ,").append(st.nextToken()); //FLUJ_EF_GEN_PROY
            query.append("  ,").append(st.nextToken()); //GARANTIAS
            query.append("  ,").append(st.nextToken()); //VAL_DESC_ACT_SUBY
            query.append("  ,").append(st.nextToken()); //VAL_ACT_SUBY
            query.append("  ,").append(st.nextToken()); //NIVEL_C
            query.append("  ,").append(st.nextToken()); //VAL_GAR_REAL_NO_FIN
            query.append("  ,").append(st.nextToken()); //VAL_DESC_GAR_REAL_NO_FIN
            query.append("  ,").append(st.nextToken()); //VAL_GAR_REAL_FIN
            query.append("  ,").append(st.nextToken()); //VAL_DESC_GAR_REAL_FIN
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //FECHA_INFO
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); //ID_PROYECTO
            
            query.append("  ,").append(st.nextToken());
            query.append("  ,").append(st.nextToken());
            query.append("  ,").append(st.nextToken());
            query.append("  ,").append(st.nextToken());
            query.append("  ,").append(st.nextToken());
            query.append("  ,").append(st.nextToken());
            query.append("  ,").append(st.nextToken());
            query.append("  ,").append(st.nextToken());
            query.append("  ,").append(st.nextToken());
            query.append("  ,").append(st.nextToken());
            query.append("  ,").append(st.nextToken());
            query.append("  ,").append(st.nextToken());
            query.append("  ,").append(st.nextToken());
            query.append("  ,").append(st.nextToken());
            
            query.append("  ,'NP'");                                                              //STATUS_CARGA
            query.append("  ,'NP'");						                  					  //STATUS_ALTA
            query.append("  ,1");                                                                 //STATUS
            query.append("  ,'").append(fechaAlta).append("'");                                   //FECHA_ALTA
            query.append("  ,'").append(idUsuario).append("'");                                   //ID_USUARIO_ALTA
            query.append("  ,'").append(macAddress).append("'");                                  //MAC_ADDRESS_ALTA
            query.append("  ,NULL");                                                              //FECHA_MODIFICACION
            query.append("  ,NULL");                                                              //ID_USUARIO_MODIFICACION
            query.append("  ,NULL)");                                                             //MAC_ADDRESS_MODIFICACION
            System.out.println(query);
            bd.executeStatement(query.toString());

            bd.commit();
            bd.close();
            bd = null;
        } catch (SQLException e) {
            bd.rollback();
            bd.close();
            bd = null;
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
            query.append("  AND ISC.TABLE_NAME = 'SICC_FALTANTES_ANEXO19' ");
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
                query.append("  ,STL.").append(campos);
                i++;
            }
            query.append(" FROM SICC_ANEXO19_LO AS STL ");
            query.append(" WHERE STL.NUMERO_CUENTA IS NOT NULL ");
            query.append("   AND EXISTS ( ");
            query.append("     SELECT SFT.NUMERO_CUENTA ");
            query.append("     FROM SICC_FALTANTES_ANEXO19 AS SFT ");
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
                    query.append("INSERT INTO dbo.LOG_SICC_FALTANTES_ANEXO19 ( "); 
                    query.append(" NUMERO_INSTITUCION "); 
                    query.append(" ,NUMERO_CUENTA "); 
                    query.append(" ,FECHA_MODIFICACION "); 
                    query.append(" ,NOMBRE_PROYECTO "); 
                    query.append(" ,DESCRIPCION_PROYECTO "); 
                    query.append(" ,SOBRE_COSTO "); 
                    query.append(" ,MONTO_CUBIERTO_TERCEROS "); 
                    query.append(" ,MESES_CONTEMPLADOS "); 
                    query.append(" ,MESES_ADICIONALES "); 
                    query.append(" ,VP_TOTAL_DEF_SUPER "); 
                    query.append(" ,UTILIDAD_PERDIDA_ACUMULADA "); 
                    query.append(" ,ETAPA "); 
                    query.append(" ,FECHA_INICIO_OPERACION "); 
                    query.append(" ,TASA_DESCUENTO_VP ");

                    query.append(" ,TIPO_PROYECTO");
					query.append(" ,POR_EXPOS_29_INST_BAN");
					query.append(" ,VP_FEGP");
					query.append(" ,ANALISIS_ESTRES");
					query.append(" ,ESTRUCTURA_FINANCIERA");
					query.append(" ,RIES_POL_ENT_REG");
					query.append(" ,RIESGO_EVENTOS_FUERZA_MAY");
					query.append(" ,ADQ_APOYOS_APROBAC");
					query.append(" ,CUMPLIMIENTO_CONTRATOS");
					query.append(" ,RIESGO_CONSTRUCCION");
					query.append(" ,TIPO_CONTRATO_CONSTRUCCION");
					query.append(" ,RIESGO_OPERATIVO");
					query.append(" ,RIESGO_SUMINISTRO");
					query.append(" ,PRENDA_ACTIVOS");
					query.append(" ,CONTROL_INST_FLUJO_EFECTIVO");
					query.append(" ,FONDOS_RESERVA");
					query.append(" ,HISTORIAL_PATROCINADOR");
					query.append(" ,COBERTURA_SEGUROS");
					query.append(" ,ACTIVO_BRGR");
					query.append(" ,ACTIVO_FA");
					query.append(" ,ACTIVO_FC");
					query.append(" ,ACTIVO_FP_FP1");
					query.append(" ,INGRESOS_PROPIOS_PROY");
					query.append(" ,GASTOS_TOTALES_PROY");
					query.append(" ,IMPACTO_FISCAL_PROY");
					query.append(" ,PAGO_DEDUDA_12MESES");
					query.append(" ,FLUJO_EFECTIVO_ADICIONAL");
					query.append(" ,P_RAZONES_FINANCIERAS");
					query.append(" ,P_ANALISIS_ESTRES");
					query.append(" ,P_ESTRUCTURA_FINANCIERA");
					query.append(" ,P_RIES_POL_ENT_REG");
					query.append(" ,P_RIESGO_EVENTOS_FUERZA_MAY");
					query.append(" ,P_ADQ_APOYOS_APROBAC");
					query.append(" ,P_CUMPLIMIENTO_CONTRATOS");
					query.append(" ,P_RIESGO_CONSTRUCCION ");
					query.append(" ,P_TIPO_CONTRATO_CONSTRUCCION ");
					query.append(" ,P_RIESGO_OPERATIVO ");
					query.append(" ,P_RIESGO_SUMINISTRO");
					query.append(" ,P_PRENDA_ACTIVOS");
					query.append(" ,P_CONTROL_INST_FLUJO_EFECTIVO");
					query.append(" ,P_FONDOS_RESERVA");
					query.append(" ,P_HISTORIAL_PATROCINADOR");
					query.append(" ,P_COBERTURA_SEGUROS");
					query.append(" ,P_ETAPA_PROYECTO");
					query.append(" ,P_ACTIVO_BRGR");
					query.append(" ,FLUJ_EF_GEN_PROY");
					query.append(" ,GARANTIAS");
					query.append(" ,VAL_DESC_ACT_SUBY");
					query.append(" ,VAL_ACT_SUBY");
					query.append(" ,NIVEL_C");
					query.append(" ,VAL_GAR_REAL_NO_FIN");
					query.append(" ,VAL_DESC_GAR_REAL_NO_FIN");
					query.append(" ,VAL_GAR_REAL_FIN");
					query.append(" ,VAL_DESC_GAR_REAL_FIN");
					query.append(" ,FECHA_INFO");
					query.append(" ,ID_PROYECTO");
                    

		            query.append(" ,DIAS_ATR_INST ");
		            query.append(" ,PORC_PE ");
		            query.append(" ,PUNT_TOT_PI_A19 ");
		            query.append(" ,PUNT_CUANT_PI_A19 ");
		            query.append(" ,PUNT_CUALI_PI_A19 ");
		            query.append(" ,PUNT_PORC_EXP_29DIAS ");
		            query.append(" ,PUNT_PORC_COB_CRED ");
		            query.append(" ,PUNT_DIAS_ATR_INST ");
		            query.append(" ,PORC_COB_CRED ");
		            query.append(" ,FACT_CONV_RC ");
		            query.append(" ,EI_AJUST_MIT ");
		            query.append(" ,EI_NETA_RES ");
		            query.append(" ,POND_RIESGO ");
		            query.append(" ,REQ_ICAP ");
					
                    query.append(" ,STATUS "); 
                    query.append(" ,ID_USUARIO_MODIFICACION "); 
                    query.append(" ,MAC_ADDRESS_MODIFICACION "); 
                    query.append(" ) "); 
                    
                    query.append(" SELECT NUMERO_INSTITUCION");
        			query.append(" ,NUMERO_CUENTA");
        			query.append(" ,'").append(fechaModificacion).append("'");
        			query.append(" ,NOMBRE_PROYECTO");
        			query.append(" ,DESCRIPCION_PROYECTO");
        			query.append(" ,SOBRE_COSTO");
        			query.append(" ,MONTO_CUBIERTO_TERCEROS");
        			query.append(" ,MESES_CONTEMPLADOS");
        			query.append(" ,MESES_ADICIONALES");
        			query.append(" ,VP_TOTAL_DEF_SUPER");
        			query.append(" ,UTILIDAD_PERDIDA_ACUMULADA");
        			query.append(" ,ETAPA");
        			query.append(" ,FECHA_INICIO_OPERACION");
        			query.append(" ,TASA_DESCUENTO_VP");
        			
        			query.append(" ,TIPO_PROYECTO");
					query.append(" ,POR_EXPOS_29_INST_BAN");
					query.append(" ,VP_FEGP");
					query.append(" ,ANALISIS_ESTRES");
					query.append(" ,ESTRUCTURA_FINANCIERA");
					query.append(" ,RIES_POL_ENT_REG");
					query.append(" ,RIESGO_EVENTOS_FUERZA_MAY");
					query.append(" ,ADQ_APOYOS_APROBAC");
					query.append(" ,CUMPLIMIENTO_CONTRATOS");
					query.append(" ,RIESGO_CONSTRUCCION");
					query.append(" ,TIPO_CONTRATO_CONSTRUCCION");
					query.append(" ,RIESGO_OPERATIVO");
					query.append(" ,RIESGO_SUMINISTRO");
					query.append(" ,PRENDA_ACTIVOS");
					query.append(" ,CONTROL_INST_FLUJO_EFECTIVO");
					query.append(" ,FONDOS_RESERVA");
					query.append(" ,HISTORIAL_PATROCINADOR");
					query.append(" ,COBERTURA_SEGUROS");
					query.append(" ,ACTIVO_BRGR");
					query.append(" ,ACTIVO_FA");
					query.append(" ,ACTIVO_FC");
					query.append(" ,ACTIVO_FP_FP1");
					query.append(" ,INGRESOS_PROPIOS_PROY");
					query.append(" ,GASTOS_TOTALES_PROY");
					query.append(" ,IMPACTO_FISCAL_PROY");
					query.append(" ,PAGO_DEDUDA_12MESES");
					query.append(" ,FLUJO_EFECTIVO_ADICIONAL");
					query.append(" ,P_RAZONES_FINANCIERAS");
					query.append(" ,P_ANALISIS_ESTRES");
					query.append(" ,P_ESTRUCTURA_FINANCIERA");
					query.append(" ,P_RIES_POL_ENT_REG");
					query.append(" ,P_RIESGO_EVENTOS_FUERZA_MAY");
					query.append(" ,P_ADQ_APOYOS_APROBAC");
					query.append(" ,P_CUMPLIMIENTO_CONTRATOS");
					query.append(" ,P_RIESGO_CONSTRUCCION ");
					query.append(" ,P_TIPO_CONTRATO_CONSTRUCCION ");
					query.append(" ,P_RIESGO_OPERATIVO ");
					query.append(" ,P_RIESGO_SUMINISTRO");
					query.append(" ,P_PRENDA_ACTIVOS");
					query.append(" ,P_CONTROL_INST_FLUJO_EFECTIVO");
					query.append(" ,P_FONDOS_RESERVA");
					query.append(" ,P_HISTORIAL_PATROCINADOR");
					query.append(" ,P_COBERTURA_SEGUROS");
					query.append(" ,P_ETAPA_PROYECTO");
					query.append(" ,P_ACTIVO_BRGR");
					query.append(" ,FLUJ_EF_GEN_PROY");
					query.append(" ,GARANTIAS");
					query.append(" ,VAL_DESC_ACT_SUBY");
					query.append(" ,VAL_ACT_SUBY");
					query.append(" ,NIVEL_C");
					query.append(" ,VAL_GAR_REAL_NO_FIN");
					query.append(" ,VAL_DESC_GAR_REAL_NO_FIN");
					query.append(" ,VAL_GAR_REAL_FIN");
					query.append(" ,VAL_DESC_GAR_REAL_FIN");
					query.append(" ,FECHA_INFO");
					query.append(" ,ID_PROYECTO");
					
		            query.append(" ,DIAS_ATR_INST ");
		            query.append(" ,PORC_PE ");
		            query.append(" ,PUNT_TOT_PI_A19 ");
		            query.append(" ,PUNT_CUANT_PI_A19 ");
		            query.append(" ,PUNT_CUALI_PI_A19 ");
		            query.append(" ,PUNT_PORC_EXP_29DIAS ");
		            query.append(" ,PUNT_PORC_COB_CRED ");
		            query.append(" ,PUNT_DIAS_ATR_INST ");
		            query.append(" ,PORC_COB_CRED ");
		            query.append(" ,FACT_CONV_RC ");
		            query.append(" ,EI_AJUST_MIT ");
		            query.append(" ,EI_NETA_RES ");
		            query.append(" ,POND_RIESGO ");
		            query.append(" ,REQ_ICAP ");

        			query.append(" ,STATUS");
        			query.append(" ,'").append(idUsuario).append("'");
        			query.append(" ,'").append(macAddress).append("'");
        			query.append(" FROM SICC_FALTANTES_ANEXO19");
        			query.append(" WHERE NUMERO_INSTITUCION=").append(numeroInstitucion);
        			query.append(" AND NUMERO_CUENTA=").append(numeroCuenta);
                    System.out.println(query);
                    bd.executeUpdate(query.toString());

                    query.setLength(0);
                    query.append("UPDATE SICC_FALTANTES_ANEXO19 ");
                    query.append("SET ");
                    query.append(updateCommon.armaUpdate(listCampos, stD));
                    query.append("  ,FECHA_MODIFICACION = '").append(fechaModificacion).append("'");
                    query.append("  ,ID_USUARIO_MODIFICACION = '").append(idUsuario).append("'");
                    query.append("  ,MAC_ADDRESS_MODIFICACION = '").append(macAddress).append("'");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  AND NUMERO_CUENTA = ").append(numeroCuenta);
                    System.out.println(query);
                    bd.executeUpdate(query.toString());

                    bd.commit();
                } catch (SQLException e) {
                    bd.rollback();
                    query.setLength(0);
                    query.append("UPDATE SICC_ANEXO19_LO ");
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
    
    public static void main(String arg[]) {
        SICC_ANEXO20 obj = new SICC_ANEXO20();
        String registro = "-123456789012345.12|";
        
         StringBuffer expRegular = new StringBuffer();
         expRegular.append("(-?\\d{0,15}+\\||-?\\d{0,15}+\\.+\\d{0,2}+\\|)");
         
         if (registro.toString().matches(expRegular.toString())) {
            System.out.println("válido!!!");
        } else {
             System.out.println("inválido!!!");
         }
    }
}

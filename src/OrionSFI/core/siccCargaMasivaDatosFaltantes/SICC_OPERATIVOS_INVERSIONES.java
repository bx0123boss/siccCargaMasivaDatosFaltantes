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

public class SICC_OPERATIVOS_INVERSIONES {

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

        expRegular.append("[a-zA-Z0-9-_ ]{0,6}+\\|"); // CLAVE_ENTIDAD
        expRegular.append("[a-zA-Z0-9-_ ]{0,7}+\\|"); // EMISORA
        expRegular.append("[a-zA-Z0-9-_ ]{0,10}+\\|"); // SERIE
        expRegular.append("[a-zA-Z0-9-_ ]{0,4}+\\|"); // TIPO_VALOR
        expRegular.append("\\d{0,2}+\\|"); // FORMA_ADQUISICION
        expRegular.append("\\d{0,3}+\\|"); // TIPO_INVERSION
        expRegular.append("\\d{0,2}+\\|"); // TIPO_INSTRUMENTO
        expRegular.append("\\d{0,12}+\\|"); // CLAS_CONTABLE
        expRegular.append("[a-zA-Z0-9-_ ]{0,60}+\\|"); // CALIFICACION
        expRegular.append("\\d{0,2}+\\|"); // FORM_AMORT
        expRegular.append("^([0-2][0-9]|3[0-1])(\\/|-)(0[1-9]|1[0-2])\\2(\\d{4})$+\\|"); // FECHA_CONTRATAC
        expRegular.append("^([0-2][0-9]|3[0-1])(\\/|-)(0[1-9]|1[0-2])\\2(\\d{4})$+\\|"); // FECHA_VENC
        expRegular.append("\\d{0,21}+\\|"); // NUMERO_TITULOS
        expRegular.append("\\d{0,21}+[\\.]?+\\d{0,2}+\\|"); // COSTO_ADQ
        expRegular.append("\\d{0,16}+[\\.]?+\\d{0,4}+\\|"); // TASA_INTERES
        expRegular.append("\\d{0,2}+\\|"); // MODELO_VALUACION
        expRegular.append("\\d{0,21}+[\\.]?+\\d{0,2}+\\|"); // VALUACION
        expRegular.append("\\d{0,21}+[\\.]?+\\d{0,2}+\\|"); // RESULTADO_VALUACION
        expRegular.append("^([0-2][0-9]|3[0-1])(\\/|-)(0[1-9]|1[0-2])\\2(\\d{4})$+\\|"); //FECHA_INFO

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
        		errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Clave de Entidad", 6, noLayout, consecutivo, (byte) 1)).append(pipe);
        	else
        		errRegistro.append(validaCampos.getCampoAlfanumerico(cad[0], "Clave de Entidad", 6, noLayout, consecutivo, (byte) 1)).append(pipe);
        } else {
            errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Clave de Entidad", 6, noLayout, consecutivo, (byte) 1)).append(pipe);
        }

        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Emisora", 7, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Serie", 10, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Tipo Valor", 4, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Forma de Adquisicion", 2, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Tipo de Inversion", 3, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Tipo de Instrumento", 2, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Clasificacion Contable", 12, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "Calificacion", 60, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Forma de Amortizacion", 2, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "Fecha de Contratacion", 10, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "Fecha de Vencimiento", 10, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Numero de Titulos", 21, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Costo de Adquisicion", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Tasa de Interes", 20, noLayout, consecutivo, cuatroDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "Modelo de Valuacion Utilizado", 2, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Valuacion", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "Resultado por Valuacion", 23, noLayout, consecutivo, dosDecimales, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "Fecha Informacion", 10, noLayout, consecutivo, (byte) 1)).append(pipe);
        
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
            query.append("INSERT INTO SICC_OPERATIVOS_INVERSIONES_LO(");
            query.append("  NUMERO_INSTITUCION");
            query.append("  ,NOMBRE_ARCHIVO");
            query.append("  ,FOLIO_LOTE");
            query.append("  ,NUMERO_LAYOUT");
            query.append("  ,NUMERO_CONSECUTIVO");
            query.append("  ,CLAVE_ENTIDAD");
            query.append("  ,EMISORA");
            query.append("  ,SERIE");
            query.append("  ,TIPO_VALOR");
            query.append("  ,FORMA_ADQUISICION");
            query.append("  ,TIPO_INVERSION");
            query.append("  ,TIPO_INSTRUMENTO");
            query.append("  ,CLAS_CONTABLE");
            query.append("  ,CALIFICACION");
            query.append("  ,FORM_AMORT");
            query.append("  ,FECHA_CONTRATAC");
            query.append("  ,FECHA_VENC");
            query.append("  ,NUMERO_TITULOS");
            query.append("  ,COSTO_ADQ");
            query.append("  ,TASA_INTERES");
            query.append("  ,MODELO_VALUACION");
            query.append("  ,VALUACION");
            query.append("  ,RESULTADO_VALUACION");
            query.append("  ,FECHA_INFO");
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
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // CLAVE_ENTIDAD
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // EMISORA
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // SERIE
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // TIPO_VALOR
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // FORMA_ADQUISICION
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // TIPO_INVERSION
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // TIPO_INSTRUMENTO
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // CLAS_CONTABLE
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // CALIFICACION
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // FORM_AMORT
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // FECHA_CONTRATAC
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // FECHA_VENC
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // NUMERO_TITULOS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // COSTO_ADQ
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // TASA_INTERES
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // MODELO_VALUACION
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // VALUACION
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // RESULTADO_VALUACION
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), cadena)); // FECHA_INFO            
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
            query.append("  AND ISC.TABLE_NAME = 'SICC_OPERATIVOS_INVERSIONES' ");
            query.append("  AND SCL.IND_ACTUALIZA = 1 ");
            query.append("  AND SCL.NUMERO_INSTITUCION = ").append(numeroInstitucion);
            query.append("  AND SCL.NUMERO_LAYOUT = ").append(noLayout);
            System.out.println(query);
            resultadoCampos = bd.executeQuery(query.toString());
            listCampos = sqlProperties.getColumnValue(resultadoCampos, listCampos);
            String campos;
            int i = 0;

            query.setLength(0);
            query.append("SELECT STL.CLAVE_ENTIDAD ");
            query.append("  ,STL.NUMERO_CONSECUTIVO ");
            while (i < listCampos.size()) {
                campos = listCampos.get(i).toString();
                campos = campos.substring(campos.indexOf("&#") + 2, campos.length());
                query.append("  ,STL.").append(campos);
                i++;
            }
            query.append(" FROM SICC_OPERATIVOS_INVERSIONES_LO AS STL ");
            query.append(" WHERE STL.CLAVE_ENTIDAD IS NOT NULL ");
            query.append("   AND EXISTS ( ");
            query.append("     SELECT SFT.CLAVE_ENTIDAD ");
            query.append("     FROM SICC_OPERATIVOS_INVERSIONES AS SFT ");
            query.append("     WHERE STL.NUMERO_INSTITUCION = SFT.NUMERO_INSTITUCION ");
            query.append("       AND STL.CLAVE_ENTIDAD = SFT.CLAVE_ENTIDAD ");
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
                    query.append("INSERT INTO LOG_SICC_OPERATIVOS_INVERSIONES (");
                    query.append("  NUMERO_INSTITUCION ");
                    query.append("  ,CLAVE_ENTIDAD");
                    query.append("  ,FECHA_MODIFICACION ");
                    query.append("  ,EMISORA");
                    query.append("  ,SERIE");
                    query.append("  ,TIPO_VALOR");
                    query.append("  ,FORMA_ADQUISICION");
                    query.append("  ,TIPO_INVERSION");
                    query.append("  ,TIPO_INSTRUMENTO");
                    query.append("  ,CLAS_CONTABLE");
                    query.append("  ,CALIFICACION");
                    query.append("  ,FORM_AMORT");
                    query.append("  ,FECHA_CONTRATAC");
                    query.append("  ,FECHA_VENC");
                    query.append("  ,NUMERO_TITULOS");
                    query.append("  ,COSTO_ADQ");
                    query.append("  ,TASA_INTERES");
                    query.append("  ,MODELO_VALUACION");
                    query.append("  ,VALUACION");
                    query.append("  ,RESULTADO_VALUACION");
                    query.append("  ,FECHA_INFO");
                    query.append("  ,STATUS ");
                    query.append("  ,ID_USUARIO_MODIFICACION ");
                    query.append("  ,MAC_ADDRESS_MODIFICACION ");
                    query.append("  ) ");
                    query.append("SELECT NUMERO_INSTITUCION ");
                    query.append("  ,CLAVE_ENTIDAD");
                    query.append("  ,'").append(fechaModificacion).append("'");
                    query.append("  ,EMISORA");
                    query.append("  ,SERIE");
                    query.append("  ,TIPO_VALOR");
                    query.append("  ,FORMA_ADQUISICION");
                    query.append("  ,TIPO_INVERSION");
                    query.append("  ,TIPO_INSTRUMENTO");
                    query.append("  ,CLAS_CONTABLE");
                    query.append("  ,CALIFICACION");
                    query.append("  ,FORM_AMORT");
                    query.append("  ,FECHA_CONTRATAC");
                    query.append("  ,FECHA_VENC");
                    query.append("  ,NUMERO_TITULOS");
                    query.append("  ,COSTO_ADQ");
                    query.append("  ,TASA_INTERES");
                    query.append("  ,MODELO_VALUACION");
                    query.append("  ,VALUACION");
                    query.append("  ,RESULTADO_VALUACION");
                    query.append("  ,FECHA_INFO");;
                    query.append("  ,STATUS ");
                    query.append("  ,'").append(idUsuario).append("'");
                    query.append("  ,'").append(macAddress).append("'");
                    query.append("FROM SICC_OPERATIVOS_INVERSIONES ");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  AND CLAVE_ENTIDAD = '").append(numeroCliente).append("'");
                    System.out.println(query);
                    bd.executeUpdate(query.toString());

                    query.setLength(0);
                    query.append("UPDATE SICC_OPERATIVOS_INVERSIONES ");
                    query.append("SET ");
                    query.append(updateCommon.armaUpdate(listCampos, stD));
                    query.append("  ,FECHA_MODIFICACION = '").append(fechaModificacion).append("'");
                    query.append("  ,ID_USUARIO_MODIFICACION = '").append(idUsuario).append("'");
                    query.append("  ,MAC_ADDRESS_MODIFICACION = '").append(macAddress).append("'");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  AND CLAVE_ENTIDAD = '").append(numeroCliente).append("'");
                    System.out.println(query);
                    bd.executeUpdate(query.toString());

                    bd.commit();
                } catch (SQLException e) {
                    bd.rollback();
                    query.setLength(0);
                    query.append("UPDATE SICC_OPERATIVOS_INVERSIONES_LO ");
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
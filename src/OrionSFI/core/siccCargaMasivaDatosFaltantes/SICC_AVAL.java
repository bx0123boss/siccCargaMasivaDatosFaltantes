/*
 **********************************************************************************************************
 *    FECHA         ID.REQUERIMIENTO            DESCRIPCIÓN                            MODIFICÓ.          *
 * 09/02/2015             3226            Creación del documento con            José Manuel Zúñiga Flores *
 *                                        especificaciones de la Carga de       Fernando Vázquez Galán    *
 *                                        Archivo para Datos Faltantes de                                 *
 *                                        Calificación de Cartera                                         *
 *                                                                                                        *
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

public class SICC_AVAL {

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
        //expRegular.append("(\\d{1,3}+\\|)");                                      //NUMERO_CONSECUTIVO
        expRegular.append("(\\d{1,15}+\\|)");                                     //NUMERO_CLIENTE
        expRegular.append("(\\d{0,3}+\\|)");                                      //PERSONALIDAD_JURIDICA
        expRegular.append("(\\d{0,3}+\\|)");                                      //TIPO_AVAL
        expRegular.append("([a-zA-Z0-9-_ +]{0,5}+\\|)");                           //CALIF_FITCH
        expRegular.append("([a-zA-Z0-9-_ +]{0,5}+\\|)");                           //CALIF_MOODYS
        expRegular.append("([a-zA-Z0-9-_ +]{0,5}+\\|)");                           //CALIF_SP
        expRegular.append("([a-zA-Z0-9-_ +]{0,5}+\\|)");                           //CALIF_HRRATINGS
        expRegular.append("([a-zA-Z0-9-_ +]{0,5}+\\|)");                           //CALIF_OTRAS
        expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)");                      //PROBABILIDAD_INCUMPLIMIENTO
//        expRegular.append("((0|1){0,1}+\\|)");                                    //ES_FONDO
        expRegular.append("([a-zA-Z0-9-_ ]{0,20}+\\|)");                          //LEI
        expRegular.append("(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,6}+\\|)");                      //PORCENTAJE
        expRegular.append("(\\d{1,1})");                                          //TIPO_COBERTURA

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
        	if (cad[0].equalsIgnoreCase("NULL")){
                errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NUMERO_CLIENTE", 15, noLayout, consecutivo, (byte) 1)).append(pipe);
            	}else{
                    errRegistro.append(validaCampos.getCampoNumerico(cad[0], "NUMERO_CLIENTE", 15, noLayout, consecutivo, (byte) 1)).append(pipe);
            	}
        } else {
            errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NUMERO_CLIENTE", 15, noLayout, consecutivo, (byte) 1)).append(pipe);
        }
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "PERSONALIDAD_JURIDICA", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TIPO_AVAL", 3, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoCalificacion(st.nextToken(), "CALIF_FITCH", 5, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoCalificacion(st.nextToken(), "CALIF_MOODYS", 5, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoCalificacion(st.nextToken(), "CALIF_SP", 5, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoCalificacion(st.nextToken(), "CALIF_HRRATINGS", 5, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoCalificacion(st.nextToken(), "CALIF_OTRAS", 5, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PROBABILIDAD_INCUMPLIMIENTO", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
//        errRegistro.append(validaCampos.getCampoBit(st.nextToken(), "ES_FONDO", 1, noLayout, consecutivo, "0", (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoAlfanumerico(st.nextToken(), "LEI", 20, noLayout, consecutivo, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PORCENTAJE", 10, noLayout, consecutivo, seisDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "TIPO_COBERTURA", 1, noLayout, consecutivo, (byte) 1)).append(pipe);

        return errRegistro.toString();
    }

    public void insertaCarga(String registro, short numeroInstitucion, String nombreArchivo, String folioLote, int noLayout, String idUsuario, String macAddress) throws Exception {
        MensajesSistema msjSistema = new MensajesSistema();
        validaCampos = new ValidaCampos();
        StringTokenizer st = new StringTokenizer(registro, "|");
        byte isNumeric = 0;
        byte isString = 1;

        SQLProperties sqlProperties = new SQLProperties();
        SimpleDateFormat sdf = new SimpleDateFormat(sqlProperties.getFormatoSoloFechaOrion());
        String fechaAlta = sqlProperties.getFechaInsercionFormateada(sdf.format(new Date()));
        try {

            bd = new JDBCConnectionPool();

            bd.setAutoCommit(false);
            query = new StringBuffer();
            query.append("INSERT INTO dbo.SICC_AVAL_LO(");
            query.append("  NUMERO_INSTITUCION");
            query.append("  ,NOMBRE_ARCHIVO");
            query.append("  ,FOLIO_LOTE");
            query.append("  ,NUMERO_LAYOUT");
            query.append("  ,NUMERO_CONSECUTIVO");
            query.append("  ,NUMERO_CLIENTE");
            query.append("  ,PERSONALIDAD_JURIDICA");
            query.append("  ,TIPO_AVAL");
            query.append("  ,CALIF_FITCH");
            query.append("  ,CALIF_MOODYS");
            query.append("  ,CALIF_SP");
            query.append("  ,CALIF_HRRATINGS");
            query.append("  ,CALIF_OTRAS");
            query.append("  ,PROBABILIDAD_INCUMPLIMIENTO");
            query.append("  ,ES_FONDO");
            query.append("  ,LEI");
            query.append("  ,PORCENTAJE");
            query.append("  ,TIPO_COBERTURA");
            query.append("  ,STATUS_CARGA");
            query.append("  ,STATUS_ALTA");
            query.append("  ,STATUS");
            query.append("  ,FECHA_ALTA");
            query.append("  ,ID_USUARIO_ALTA");
            query.append("  ,MAC_ADDRESS_ALTA");
            query.append("  ,FECHA_MODIFICACION");
            query.append("  ,ID_USUARIO_MODIFICACION");
            query.append("  ,MAC_ADDRESS_MODIFICACION) ");
            query.append("VALUES (");
            query.append("  ").append(numeroInstitucion);			                  //NUMERO_INSTITUCION
            query.append("  ,'").append(nombreArchivo).append("'");                               //NOMBRE_ARCHIVO
            query.append("  ,'").append(folioLote).append("'");                                   //FOLIO_LOTE
            query.append("  ,").append(noLayout);                                                 //NUMERO_LAYOUT
            query.append("  ,").append(st.nextToken());                                           //NUMERO_CONSECUTIVO
            query.append("  ,").append(st.nextToken());                                           //NUMERO_CLIENTE
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //PERSONALIDAD_JURIDICA
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //TIPO_AVAL
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //CALIF_FITCH
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //CALIF_MOODYS
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //CALIF_SP
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //CALIF_HRRATINGS
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //CALIF_OTRAS
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //PROBABILIDAD_INCUMPLIMIENTO
//            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //ES_FONDO
            query.append("  ,NULL "); //ES_FONDO
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isString));  //LEI
            query.append("  ,").append(validaCampos.validaStringNull(st.nextToken(), isNumeric)); //PORCENTAJE
            query.append("  ,").append(st.nextToken());                                           //TIPO_COBERTURA
            query.append("  ,'NP'");                                             //STATUS_CARGA
            query.append("  ,'NP'");                                             //STATUS_ALTA
            query.append("  ,1");                                                //STATUS
            query.append("  ,'").append(fechaAlta).append("'");                                        //FECHA_ALTA
            query.append("  ,'").append(idUsuario).append("'");                  //ID_USUARIO_ALTA
            query.append("  ,'").append(macAddress).append("'");                 //MAC_ADDRESS_ALTA
            query.append("  ,NULL");                                             //FECHA_MODIFICACION
            query.append("  ,NULL");                                             //ID_USUARIO_MODIFICACION
            query.append("  ,NULL)");                                            //MAC_ADDRESS_MODIFICACION
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
            query.append("  AND ISC.TABLE_NAME = 'SICC_FALTANTES_AVAL' ");
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
            query.append(" FROM SICC_AVAL_LO AS STL ");
            query.append(" WHERE STL.NUMERO_CLIENTE IS NOT NULL ");
            query.append("   AND EXISTS ( ");
            query.append("     SELECT SFT.NUMERO_CLIENTE ");
            query.append("     FROM SICC_FALTANTES_AVAL AS SFT ");
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
                    query.append("INSERT INTO LOG_SICC_FALTANTES_AVAL (");
                    query.append("  NUMERO_INSTITUCION ");
                    query.append("  ,NUMERO_CLIENTE ");
                    query.append("  ,FECHA_MODIFICACION ");
                    query.append("  ,PERSONALIDAD_JURIDICA ");
                    query.append("  ,TIPO_AVAL ");
                    query.append("  ,CALIF_FITCH ");
                    query.append("  ,CALIF_MOODYS ");
                    query.append("  ,CALIF_SP ");
                    query.append("  ,CALIF_HRRATINGS ");
                    query.append("  ,CALIF_OTRAS ");
                    query.append("  ,PROBABILIDAD_INCUMPLIMIENTO ");
                    query.append("  ,ES_FONDO ");
                    query.append("  ,LEI ");
                    query.append("  ,PORCENTAJE ");
                    query.append("  ,TIPO_COBERTURA ");
                    query.append("  ,STATUS ");
                    query.append("  ,ID_USUARIO_MODIFICACION ");
                    query.append("  ,MAC_ADDRESS_MODIFICACION ");
                    query.append("  ) ");
                    query.append("SELECT NUMERO_INSTITUCION ");
                    query.append("  ,NUMERO_CLIENTE ");
                    query.append("  ,'").append(fechaModificacion).append("'");
                    query.append("  ,PERSONALIDAD_JURIDICA ");
                    query.append("  ,TIPO_AVAL ");
                    query.append("  ,CALIF_FITCH ");
                    query.append("  ,CALIF_MOODYS ");
                    query.append("  ,CALIF_SP ");
                    query.append("  ,CALIF_HRRATINGS ");
                    query.append("  ,CALIF_OTRAS ");
                    query.append("  ,PROBABILIDAD_INCUMPLIMIENTO ");
                    query.append("  ,ES_FONDO ");
                    query.append("  ,LEI ");
                    query.append("  ,PORCENTAJE ");
                    query.append("  ,TIPO_COBERTURA ");
                    query.append("  ,STATUS ");
                    query.append("  ,'").append(idUsuario).append("'");
                    query.append("  ,'").append(macAddress).append("'");
                    query.append("FROM SICC_FALTANTES_AVAL ");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  AND NUMERO_CLIENTE = ").append(numeroCliente);
                    System.out.println(query);
                    bd.executeUpdate(query.toString());

                    query.setLength(0);
                    query.append("UPDATE SICC_FALTANTES_AVAL ");
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
                    query.append("UPDATE SICC_AVAL_LO ");
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

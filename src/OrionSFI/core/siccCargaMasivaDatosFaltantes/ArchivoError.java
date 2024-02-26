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

import OrionSFI.core.commons.JDBCConnectionPool;
import OrionSFI.core.commons.MensajesSistema;
import OrionSFI.core.commons.SQLProperties;
import OrionSFI.core.institucion.Institucion;
import OrionSFI.core.sistema.Sistema;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ArchivoError {

    private JDBCConnectionPool bd = null;
    private StringBuffer query = null;
    private ResultSet resultado = null;
    private short noInstitucion = 0;
    private String folioLote = null;
    private String idUsuario = null;
    private String macAddress = null;
    private String fechaSistema = null;

    public ArchivoError(short _numInstitucion, String _idUsuario, String _macAddress, String _folioLote) throws Exception {
        Institucion institucion = new Institucion();
        this.noInstitucion = _numInstitucion;
        this.idUsuario = _idUsuario;
        this.macAddress = _macAddress;
        this.folioLote = _folioLote;
        this.fechaSistema = institucion.getFechaSistema(noInstitucion, Sistema.sistema.CREDITO);
    }

    public ArchivoError() {
    }

    /**
     * Inserta errores a tabla log
     *
     * @param cadena
     * @param error
     * @param noLayout
     * @throws Exception
     */
    public void insertaError(String cadena, int error, int noLayout, int consecutivo) throws Exception {
        MensajesSistema msjSistema = new MensajesSistema();
        SQLProperties sqlProperties = new SQLProperties();
        SimpleDateFormat sdf = new SimpleDateFormat(sqlProperties.getFormatoSoloFechaOrion());
        String fechaAlta = sqlProperties.getFechaInsercionFormateada(sdf.format(new Date()));
        String mensaje;
        try {
            mensaje = msjSistema.getMensaje(error);
            if (cadena != null && !cadena.trim().isEmpty() && error > 0) {
                mensaje = mensaje.replace("&#", cadena);
                mensaje = mensaje.replace("'", "");
            }

            bd = new JDBCConnectionPool();

            query = new StringBuffer();
            query.append("SELECT ISNULL(MAX(CONSECUTIVO_ERROR) + 1, 1)");
            query.append("FROM SICC_CARGA_ERRORES ");
            query.append("WHERE NUMERO_INSTITUCION = ").append(noInstitucion);
            query.append(" AND NUMERO_FOLIO = '").append(folioLote).append("'");
            query.append(" AND NUMERO_LAYOUT = ").append(noLayout);
            query.append(" AND STATUS = 1");
            System.out.println(query);
            resultado = bd.executeQuery(query.toString());

            int consecutivoError = 0;
            if (resultado.next()) {
                consecutivoError = resultado.getInt(1);
            }

            //inserta en el log
            query.setLength(0);
            query.append("INSERT INTO SICC_CARGA_ERRORES(");
            query.append("  NUMERO_INSTITUCION");
            query.append("  ,NUMERO_LAYOUT");
            query.append("  ,NUMERO_FOLIO");
            query.append("  ,FECHA_PROCESO");
            query.append("  ,NUMERO_CONSECUTIVO");
            query.append("  ,CONSECUTIVO_ERROR");
            query.append("  ,CODIGO_MENSAJE");
            query.append("  ,DESCRIPCION_ERROR");
            query.append("  ,STATUS");
            query.append("  ,FECHA_ALTA");
            query.append("  ,ID_USUARIO_ALTA");
            query.append("  ,MAC_ADDRESS_ALTA");
            query.append("  ,FECHA_MODIFICACION");
            query.append("  ,ID_USUARIO_MODIFICACION");
            query.append("  ,MAC_ADDRESS_MODIFICACION) ");
            query.append("VALUES(");
            query.append("  ").append(noInstitucion);
            query.append("  ,").append(noLayout);
            query.append("  ,'").append(folioLote).append("'");
            query.append("  ,'").append(fechaSistema).append("'");
            query.append("  ,").append(consecutivo);
            query.append("  ,").append(consecutivoError);
            query.append("  ,").append(error);
            query.append("  ,'").append(mensaje).append("'");
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
            bd = null;

        } catch (SQLException e) {
            bd.rollback();
            bd.close();
            bd = null;
            e.getStackTrace();
            throw new Exception(msjSistema.getMensaje(131));
        }
    }
}

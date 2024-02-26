
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

public class SICC_ESTADO_CUENTA {

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

        expRegular.append("\\d{1,22}+\\|"); // NUMERO_CLIENTE
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // CAJA_BANCOS_INVER
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // CUENTAS_COBRAR
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // SEMOVIENTES_CULTIVOS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // FUNCIONARIOS_EMPLEADOS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // CUENTAS_INCOBRABLES
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // ANTICIPO_PROVEEDORES
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // SUBSIDIOS_EMPLEO
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // PAGOS_ANTICIPADOS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // OTROS_INSUMOS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // ALMACEN_ANTICIPOS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // DEUDORES_DIVERSOS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // IVA_ACREDITABLE
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // IMPUESTOS_RECUPERAR
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // INVENTARIOS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // ANTICIPO_IMPUESTOS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // CUENTAS_COBRAR_PARTES_REL
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // RETENCIONES
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // OTROS_ACTIVOS_CIRCULANTES
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // INV_SUBSIDIARIA_OTRAS_INV
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // ACTIVO_FIJO
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // DEPRECIACION_ACUMULADA
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // DEPOSITOS_GARANTIA
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // PAGOS_ANTICIPADOSF
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // IMPUESTOS_ANTICIPADOS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // ISR_DIFERIDO
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // AMORT_MEJORAS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // GASTOS_DIFERIDOS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // CREDITO_DIESEL
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // INTERESES_OTROS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // DIVERSOS_PASIVOS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // SUELDO_SALARIO_PENDIENTES
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // DOCUMENTOS_PAGAR
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // PROVEEDORES
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // CUENTAS_PAGAR_ANTICIPOS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // IVA_PAGAR_PENDIENTE
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // ANTICIPO_CLIENTES
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // PARTES_RELACIONADAS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // ANTICIPO_CLIENTES2
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // PROVISIONES
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // PTU
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // DEPOSITOS_GARANTIA_2
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // IMPUESTOS_PAGAR
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // ACREEDORES_DIVERSOS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // PASIVOS_LABORALES
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // DOCUMENTOS_PAGAR_PASIVO
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // IETU_DIFERIDO
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // APORTACION_INV_CAPITALIZABLE
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // ACREEDORES_DIVERSOS_PASIVO
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // CAPITAL_SOCIAL
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // APORTACIONES_FUTUROS_CAPITAL
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // PERDIDAS_ACUMULADAS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // PERDIDA_EJERCICIO
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // RESULTADO_ACUMULADO_ISR
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // INGRESOS_VENTAS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // COSTO_VENTAS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // UTILIDAD_BRUTA
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // GASTOS_ADMINISTRACION
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // OTROS_PRODUCTOS
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // PRODUCTO_PERDIDA_FINANCIERA
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // PARTICIPACION_SUBSIDIARIA
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // DEP_CONTABLE
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // UTILIDAD_NETA_EJERCICIO
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // RECURSOS_GENERADOS_OPE
        expRegular.append("\\d{1,21}+[\\.]?+\\d{0,2}+\\|"); // RETIRO_SOCIOS

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

        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "CAJA_BANCOS_INVER", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "CUENTAS_COBRAR", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SEMOVIENTES_CULTIVOS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "FUNCIONARIOS_EMPLEADOS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "CUENTAS_INCOBRABLES", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "ANTICIPO_PROVEEDORES", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SUBSIDIOS_EMPLEO", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PAGOS_ANTICIPADOS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "OTROS_INSUMOS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "ALMACEN_ANTICIPOS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "DEUDORES_DIVERSOS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "IVA_ACREDITABLE", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "IMPUESTOS_RECUPERAR", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "INVENTARIOS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "ANTICIPO_IMPUESTOS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "CUENTAS_COBRAR_PARTES_REL", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "RETENCIONES", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "OTROS_ACTIVOS_CIRCULANTES", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "INV_SUBSIDIARIA_OTRAS_INV", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "ACTIVO_FIJO", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "DEPRECIACION_ACUMULADA", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "DEPOSITOS_GARANTIA", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PAGOS_ANTICIPADOSF", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "IMPUESTOS_ANTICIPADOS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "ISR_DIFERIDO", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "AMORT_MEJORAS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "GASTOS_DIFERIDOS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "CREDITO_DIESEL", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "INTERESES_OTROS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "DIVERSOS_PASIVOS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "SUELDO_SALARIO_PENDIENTES", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "DOCUMENTOS_PAGAR", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PROVEEDORES", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "CUENTAS_PAGAR_ANTICIPOS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "IVA_PAGAR_PENDIENTE", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "ANTICIPO_CLIENTES", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PARTES_RELACIONADAS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "ANTICIPO_CLIENTES2", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PROVISIONES", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PTU", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "DEPOSITOS_GARANTIA_2", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "IMPUESTOS_PAGAR", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "ACREEDORES_DIVERSOS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PASIVOS_LABORALES", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "DOCUMENTOS_PAGAR_PASIVO", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "IETU_DIFERIDO", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "APORTACION_INV_CAPITALIZABLE", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "ACREEDORES_DIVERSOS_PASIVO", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "CAPITAL_SOCIAL", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "APORTACIONES_FUTUROS_CAPITAL", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PERDIDAS_ACUMULADAS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PERDIDA_EJERCICIO", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "RESULTADO_ACUMULADO_ISR", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "INGRESOS_VENTAS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "COSTO_VENTAS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "UTILIDAD_BRUTA", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "GASTOS_ADMINISTRACION", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "OTROS_PRODUCTOS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PRODUCTO_PERDIDA_FINANCIERA", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "PARTICIPACION_SUBSIDIARIA", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "DEP_CONTABLE", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "UTILIDAD_NETA_EJERCICIO", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "RECURSOS_GENERADOS_OPE", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        errRegistro.append(validaCampos.getCampoNumericoDecimales(st.nextToken(), "RETIRO_SOCIOS", 23, noLayout, consecutivo, dosDecimales, (byte) 0)).append(pipe);
        
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
            query.append("INSERT INTO SICC_ESTADO_CUENTA_LO(");
            query.append("  NUMERO_INSTITUCION");
            query.append(" ,NOMBRE_ARCHIVO");
            query.append(" ,FOLIO_LOTE");
            query.append(" ,NUMERO_LAYOUT");
            query.append(" ,NUMERO_CONSECUTIVO");
            query.append(" ,NUMERO_CLIENTE");
            query.append(" ,CAJA_BANCOS_INVER ");
            query.append(" ,CUENTAS_COBRAR ");
            query.append(" ,SEMOVIENTES_CULTIVOS ");
            query.append(" ,FUNCIONARIOS_EMPLEADOS ");
            query.append(" ,CUENTAS_INCOBRABLES ");
            query.append(" ,ANTICIPO_PROVEEDORES ");
            query.append(" ,SUBSIDIOS_EMPLEO ");
            query.append(" ,PAGOS_ANTICIPADOS ");
            query.append(" ,OTROS_INSUMOS ");
            query.append(" ,ALMACEN_ANTICIPOS ");
            query.append(" ,DEUDORES_DIVERSOS ");
            query.append(" ,IVA_ACREDITABLE ");
            query.append(" ,IMPUESTOS_RECUPERAR ");
            query.append(" ,INVENTARIOS ");
            query.append(" ,ANTICIPO_IMPUESTOS ");
            query.append(" ,CUENTAS_COBRAR_PARTES_REL ");
            query.append(" ,RETENCIONES ");
            query.append(" ,OTROS_ACTIVOS_CIRCULANTES ");
            query.append(" ,INV_SUBSIDIARIA_OTRAS_INV ");
            query.append(" ,ACTIVO_FIJO ");
            query.append(" ,DEPRECIACION_ACUMULADA ");
            query.append(" ,DEPOSITOS_GARANTIA ");
            query.append(" ,PAGOS_ANTICIPADOSF ");
            query.append(" ,IMPUESTOS_ANTICIPADOS ");
            query.append(" ,ISR_DIFERIDO ");
            query.append(" ,AMORT_MEJORAS ");
            query.append(" ,GASTOS_DIFERIDOS ");
            query.append(" ,CREDITO_DIESEL ");
            query.append(" ,INTERESES_OTROS ");
            query.append(" ,DIVERSOS_PASIVOS ");
            query.append(" ,SUELDO_SALARIO_PENDIENTES ");
            query.append(" ,DOCUMENTOS_PAGAR ");
            query.append(" ,PROVEEDORES ");
            query.append(" ,CUENTAS_PAGAR_ANTICIPOS ");
            query.append(" ,IVA_PAGAR_PENDIENTE ");
            query.append(" ,ANTICIPO_CLIENTES ");
            query.append(" ,PARTES_RELACIONADAS ");
            query.append(" ,ANTICIPO_CLIENTES2 ");
            query.append(" ,PROVISIONES ");
            query.append(" ,PTU ");
            query.append(" ,DEPOSITOS_GARANTIA_2 ");
            query.append(" ,IMPUESTOS_PAGAR ");
            query.append(" ,ACREEDORES_DIVERSOS ");
            query.append(" ,PASIVOS_LABORALES ");
            query.append(" ,DOCUMENTOS_PAGAR_PASIVO ");
            query.append(" ,IETU_DIFERIDO ");
            query.append(" ,APORTACION_INV_CAPITALIZABLE ");
            query.append(" ,ACREEDORES_DIVERSOS_PASIVO ");
            query.append(" ,CAPITAL_SOCIAL ");
            query.append(" ,APORTACIONES_FUTUROS_CAPITAL ");
            query.append(" ,PERDIDAS_ACUMULADAS ");
            query.append(" ,PERDIDA_EJERCICIO ");
            query.append(" ,RESULTADO_ACUMULADO_ISR ");
            query.append(" ,INGRESOS_VENTAS ");
            query.append(" ,COSTO_VENTAS ");
            query.append(" ,UTILIDAD_BRUTA ");
            query.append(" ,GASTOS_ADMINISTRACION ");
            query.append(" ,OTROS_PRODUCTOS ");
            query.append(" ,PRODUCTO_PERDIDA_FINANCIERA ");
            query.append(" ,PARTICIPACION_SUBSIDIARIA ");
            query.append(" ,DEP_CONTABLE ");
            query.append(" ,UTILIDAD_NETA_EJERCICIO ");
            query.append(" ,RECURSOS_GENERADOS_OPE ");
            query.append(" ,RETIRO_SOCIOS ");
            
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
            query.append("  ,'").append(nombreArchivo).append("'");
            query.append("  ,'").append(folioLote).append("'");
            query.append("  ,").append(noLayout);
            query.append("  ,").append(st.nextToken()); // NUMERO_CONSECUTIVO
            query.append("  ,").append(st.nextToken()); // NUMERO_CLIENTE
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // CAJA_BANCOS_INVER
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // CUENTAS_COBRAR
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // SEMOVIENTES_CULTIVOS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // FUNCIONARIOS_EMPLEADOS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // CUENTAS_INCOBRABLES
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // ANTICIPO_PROVEEDORES
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // SUBSIDIOS_EMPLEO
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // PAGOS_ANTICIPADOS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // OTROS_INSUMOS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // ALMACEN_ANTICIPOS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // DEUDORES_DIVERSOS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // IVA_ACREDITABLE
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // IMPUESTOS_RECUPERAR
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // INVENTARIOS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // ANTICIPO_IMPUESTOS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // CUENTAS_COBRAR_PARTES_REL
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // RETENCIONES
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // OTROS_ACTIVOS_CIRCULANTES
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // INV_SUBSIDIARIA_OTRAS_INV
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // ACTIVO_FIJO
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // DEPRECIACION_ACUMULADA
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // DEPOSITOS_GARANTIA
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // PAGOS_ANTICIPADOSF
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // IMPUESTOS_ANTICIPADOS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // ISR_DIFERIDO
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // AMORT_MEJORAS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // GASTOS_DIFERIDOS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // CREDITO_DIESEL
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // INTERESES_OTROS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // DIVERSOS_PASIVOS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // SUELDO_SALARIO_PENDIENTES
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // DOCUMENTOS_PAGAR
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // PROVEEDORES
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // CUENTAS_PAGAR_ANTICIPOS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // IVA_PAGAR_PENDIENTE
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // ANTICIPO_CLIENTES
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // PARTES_RELACIONADAS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // ANTICIPO_CLIENTES2
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // PROVISIONES
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // PTU
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // DEPOSITOS_GARANTIA_2
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // IMPUESTOS_PAGAR
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // ACREEDORES_DIVERSOS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // PASIVOS_LABORALES
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // DOCUMENTOS_PAGAR_PASIVO
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // IETU_DIFERIDO
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // APORTACION_INV_CAPITALIZABLE
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // ACREEDORES_DIVERSOS_PASIVO
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // CAPITAL_SOCIAL
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // APORTACIONES_FUTUROS_CAPITAL
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // PERDIDAS_ACUMULADAS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // PERDIDA_EJERCICIO
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // RESULTADO_ACUMULADO_ISR
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // INGRESOS_VENTAS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // COSTO_VENTAS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // UTILIDAD_BRUTA
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // GASTOS_ADMINISTRACION
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // OTROS_PRODUCTOS
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // PRODUCTO_PERDIDA_FINANCIERA
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // PARTICIPACION_SUBSIDIARIA
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // DEP_CONTABLE
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // UTILIDAD_NETA_EJERCICIO
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // RECURSOS_GENERADOS_OPE
            query.append("	,").append(validaCampos.validaStringNull(st.nextToken(), numero)); // RETIRO_SOCIOS
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
            query.append("  AND ISC.TABLE_NAME = 'SICC_ESTADO_CUENTA' ");
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
            query.append(" FROM SICC_ESTADO_CUENTA_LO AS STL ");
            query.append(" WHERE STL.NUMERO_CLIENTE IS NOT NULL ");
            query.append("   AND EXISTS ( ");
            query.append("     SELECT SFT.NUMERO_CLIENTE ");
            query.append("     FROM SICC_ESTADO_CUENTA AS SFT ");
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
                    query.append("INSERT INTO LOG_ESTADO_CUENTA (");
                    query.append("  NUMERO_INSTITUCION ");
                    query.append(" ,FECHA_MODIFICACION ");
                    query.append(" ,NUMERO_CLIENTE ");
                    query.append(" ,CAJA_BANCOS_INVER ");
                    query.append(" ,CUENTAS_COBRAR ");
                    query.append(" ,SEMOVIENTES_CULTIVOS ");
                    query.append(" ,FUNCIONARIOS_EMPLEADOS ");
                    query.append(" ,CUENTAS_INCOBRABLES ");
                    query.append(" ,ANTICIPO_PROVEEDORES ");
                    query.append(" ,SUBSIDIOS_EMPLEO ");
                    query.append(" ,PAGOS_ANTICIPADOS ");
                    query.append(" ,OTROS_INSUMOS ");
                    query.append(" ,ALMACEN_ANTICIPOS ");
                    query.append(" ,DEUDORES_DIVERSOS ");
                    query.append(" ,IVA_ACREDITABLE ");
                    query.append(" ,IMPUESTOS_RECUPERAR ");
                    query.append(" ,INVENTARIOS ");
                    query.append(" ,ANTICIPO_IMPUESTOS ");
                    query.append(" ,CUENTAS_COBRAR_PARTES_REL ");
                    query.append(" ,RETENCIONES ");
                    query.append(" ,OTROS_ACTIVOS_CIRCULANTES ");
                    query.append(" ,INV_SUBSIDIARIA_OTRAS_INV ");
                    query.append(" ,ACTIVO_FIJO ");
                    query.append(" ,DEPRECIACION_ACUMULADA ");
                    query.append(" ,DEPOSITOS_GARANTIA ");
                    query.append(" ,PAGOS_ANTICIPADOSF ");
                    query.append(" ,IMPUESTOS_ANTICIPADOS ");
                    query.append(" ,ISR_DIFERIDO ");
                    query.append(" ,AMORT_MEJORAS ");
                    query.append(" ,GASTOS_DIFERIDOS ");
                    query.append(" ,CREDITO_DIESEL ");
                    query.append(" ,INTERESES_OTROS ");
                    query.append(" ,DIVERSOS_PASIVOS ");
                    query.append(" ,SUELDO_SALARIO_PENDIENTES ");
                    query.append(" ,DOCUMENTOS_PAGAR ");
                    query.append(" ,PROVEEDORES ");
                    query.append(" ,CUENTAS_PAGAR_ANTICIPOS ");
                    query.append(" ,IVA_PAGAR_PENDIENTE ");
                    query.append(" ,ANTICIPO_CLIENTES ");
                    query.append(" ,PARTES_RELACIONADAS ");
                    query.append(" ,ANTICIPO_CLIENTES2 ");
                    query.append(" ,PROVISIONES ");
                    query.append(" ,PTU ");
                    query.append(" ,DEPOSITOS_GARANTIA_2 ");
                    query.append(" ,IMPUESTOS_PAGAR ");
                    query.append(" ,ACREEDORES_DIVERSOS ");
                    query.append(" ,PASIVOS_LABORALES ");
                    query.append(" ,DOCUMENTOS_PAGAR_PASIVO ");
                    query.append(" ,IETU_DIFERIDO ");
                    query.append(" ,APORTACION_INV_CAPITALIZABLE ");
                    query.append(" ,ACREEDORES_DIVERSOS_PASIVO ");
                    query.append(" ,CAPITAL_SOCIAL ");
                    query.append(" ,APORTACIONES_FUTUROS_CAPITAL ");
                    query.append(" ,PERDIDAS_ACUMULADAS ");
                    query.append(" ,PERDIDA_EJERCICIO ");
                    query.append(" ,RESULTADO_ACUMULADO_ISR ");
                    query.append(" ,INGRESOS_VENTAS ");
                    query.append(" ,COSTO_VENTAS ");
                    query.append(" ,UTILIDAD_BRUTA ");
                    query.append(" ,GASTOS_ADMINISTRACION ");
                    query.append(" ,OTROS_PRODUCTOS ");
                    query.append(" ,PRODUCTO_PERDIDA_FINANCIERA ");
                    query.append(" ,PARTICIPACION_SUBSIDIARIA ");
                    query.append(" ,DEP_CONTABLE ");
                    query.append(" ,UTILIDAD_NETA_EJERCICIO ");
                    query.append(" ,RECURSOS_GENERADOS_OPE ");
                    query.append(" ,RETIRO_SOCIOS ");
                    
                    query.append("  ,STATUS ");
                    query.append("  ,ID_USUARIO_MODIFICACION ");
                    query.append("  ,MAC_ADDRESS_MODIFICACION ");
                    query.append("  ) ");
                    query.append("SELECT NUMERO_INSTITUCION ");
                    query.append("  ,'").append(fechaModificacion).append("'");
                    query.append("  ,NUMERO_CLIENTE ");
                    query.append(" ,CAJA_BANCOS_INVER ");
                    query.append(" ,CUENTAS_COBRAR ");
                    query.append(" ,SEMOVIENTES_CULTIVOS ");
                    query.append(" ,FUNCIONARIOS_EMPLEADOS ");
                    query.append(" ,CUENTAS_INCOBRABLES ");
                    query.append(" ,ANTICIPO_PROVEEDORES ");
                    query.append(" ,SUBSIDIOS_EMPLEO ");
                    query.append(" ,PAGOS_ANTICIPADOS ");
                    query.append(" ,OTROS_INSUMOS ");
                    query.append(" ,ALMACEN_ANTICIPOS ");
                    query.append(" ,DEUDORES_DIVERSOS ");
                    query.append(" ,IVA_ACREDITABLE ");
                    query.append(" ,IMPUESTOS_RECUPERAR ");
                    query.append(" ,INVENTARIOS ");
                    query.append(" ,ANTICIPO_IMPUESTOS ");
                    query.append(" ,CUENTAS_COBRAR_PARTES_REL ");
                    query.append(" ,RETENCIONES ");
                    query.append(" ,OTROS_ACTIVOS_CIRCULANTES ");
                    query.append(" ,INV_SUBSIDIARIA_OTRAS_INV ");
                    query.append(" ,ACTIVO_FIJO ");
                    query.append(" ,DEPRECIACION_ACUMULADA ");
                    query.append(" ,DEPOSITOS_GARANTIA ");
                    query.append(" ,PAGOS_ANTICIPADOSF ");
                    query.append(" ,IMPUESTOS_ANTICIPADOS ");
                    query.append(" ,ISR_DIFERIDO ");
                    query.append(" ,AMORT_MEJORAS ");
                    query.append(" ,GASTOS_DIFERIDOS ");
                    query.append(" ,CREDITO_DIESEL ");
                    query.append(" ,INTERESES_OTROS ");
                    query.append(" ,DIVERSOS_PASIVOS ");
                    query.append(" ,SUELDO_SALARIO_PENDIENTES ");
                    query.append(" ,DOCUMENTOS_PAGAR ");
                    query.append(" ,PROVEEDORES ");
                    query.append(" ,CUENTAS_PAGAR_ANTICIPOS ");
                    query.append(" ,IVA_PAGAR_PENDIENTE ");
                    query.append(" ,ANTICIPO_CLIENTES ");
                    query.append(" ,PARTES_RELACIONADAS ");
                    query.append(" ,ANTICIPO_CLIENTES2 ");
                    query.append(" ,PROVISIONES ");
                    query.append(" ,PTU ");
                    query.append(" ,DEPOSITOS_GARANTIA_2 ");
                    query.append(" ,IMPUESTOS_PAGAR ");
                    query.append(" ,ACREEDORES_DIVERSOS ");
                    query.append(" ,PASIVOS_LABORALES ");
                    query.append(" ,DOCUMENTOS_PAGAR_PASIVO ");
                    query.append(" ,IETU_DIFERIDO ");
                    query.append(" ,APORTACION_INV_CAPITALIZABLE ");
                    query.append(" ,ACREEDORES_DIVERSOS_PASIVO ");
                    query.append(" ,CAPITAL_SOCIAL ");
                    query.append(" ,APORTACIONES_FUTUROS_CAPITAL ");
                    query.append(" ,PERDIDAS_ACUMULADAS ");
                    query.append(" ,PERDIDA_EJERCICIO ");
                    query.append(" ,RESULTADO_ACUMULADO_ISR ");
                    query.append(" ,INGRESOS_VENTAS ");
                    query.append(" ,COSTO_VENTAS ");
                    query.append(" ,UTILIDAD_BRUTA ");
                    query.append(" ,GASTOS_ADMINISTRACION ");
                    query.append(" ,OTROS_PRODUCTOS ");
                    query.append(" ,PRODUCTO_PERDIDA_FINANCIERA ");
                    query.append(" ,PARTICIPACION_SUBSIDIARIA ");
                    query.append(" ,DEP_CONTABLE ");
                    query.append(" ,UTILIDAD_NETA_EJERCICIO ");
                    query.append(" ,RECURSOS_GENERADOS_OPE ");
                    query.append(" ,RETIRO_SOCIOS ");;

                    query.append("  ,STATUS ");
                    query.append("  ,'").append(idUsuario).append("'");
                    query.append("  ,'").append(macAddress).append("'");
                    query.append("FROM SICC_ESTADO_CUENTA ");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  AND NUMERO_CLIENTE = ").append(numeroCliente);
                    System.out.println(query);
                    bd.executeUpdate(query.toString());

                    query.setLength(0);
                    query.append("UPDATE SICC_ESTADO_CUENTA ");
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
                    query.append("UPDATE ESTADO_CUENTA_LO ");
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
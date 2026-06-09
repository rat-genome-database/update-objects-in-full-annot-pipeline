package edu.mcw.rgd;

import edu.mcw.rgd.dao.AbstractDAO;
import edu.mcw.rgd.process.MemoryMonitor;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Program to update OBJECT_SYMBOL(gene symbol) and OBJECT_NAME(gene name) in the FULL_ANNOT table
 * for all genes, strains, qtls and clinvar variants which have the same ANNOTATED_OBJECT_RGD_ID
 * with the RGD_ID field in the GENES table.
 */
public class UpdateObjectsInFullAnnot {

    Logger log = LogManager.getLogger("status");
    AbstractDAO dao = new AbstractDAO();

    private String version;
    private int lastModifiedBy;

    public static void main(String[] args) throws Exception {

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        UpdateObjectsInFullAnnot instance = (UpdateObjectsInFullAnnot) (bf.getBean("manager"));

        try {
            instance.run();
        } catch(Exception e) {
            Utils.printStackTrace(e, instance.log);
            throw e;
        }
    }

    public void run() throws Exception {

        long time0 = System.currentTimeMillis();

        MemoryMonitor memoryMonitor = new MemoryMonitor();
        memoryMonitor.start();

        log.info(getVersion());
        log.info("   "+dao.getConnectionInfo());
        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        log.info("   started at "+sdt.format(new java.util.Date(time0)));
        log.info("=======");

        List<ObjType> objTypes = List.of(
                new ObjType("GENES",            1,  "GENES",            "GENE_SYMBOL",   "FULL_NAME"),
                new ObjType("STRAINS",          5,  "STRAINS",          "STRAIN_SYMBOL", "FULL_NAME"),
                new ObjType("QTLS",             6,  "QTLS",             "QTL_SYMBOL",    "QTL_NAME"),
                new ObjType("CLINVAR VARIANTS", 7,  "GENOMIC_ELEMENTS", "SYMBOL",        "NAME"),
                new ObjType("CELL LINES",       11, "GENOMIC_ELEMENTS", "SYMBOL",        "NAME")
        );

        try( Connection conn = dao.getConnection();
             Statement stmt = conn.createStatement() ) {
            for( ObjType t: objTypes ) {
                updateObjectType(stmt, t);
            }
        }

        memoryMonitor.stop();
        log.info(memoryMonitor.getSummary());
        log.info("");
        log.info("=== OK ===  elapsed  "+Utils.formatElapsedTime(time0, System.currentTimeMillis()));
        log.info("");
    }

    /** an object type whose FULL_ANNOT symbol/name is resynced from its source table */
    record ObjType(String label, int objectKey, String table, String symbolCol, String nameCol) {}

    void updateObjectType(Statement stmt, ObjType t) throws Exception {

        // table/column names come from the hardcoded list in run(), not from user input
        String sql = """
            SELECT f.OBJECT_SYMBOL, f.OBJECT_NAME, f.ANNOTATED_OBJECT_RGD_ID, f.FULL_ANNOT_KEY,
                   o.%s object_symbol2, o.%s object_name2
            FROM FULL_ANNOT f, %s o
            WHERE f.RGD_OBJECT_KEY=%d AND f.ANNOTATED_OBJECT_RGD_ID = o.RGD_ID
              AND (NVL(f.OBJECT_SYMBOL,'*')<>NVL(o.%s,'*') OR (NVL(f.OBJECT_NAME,'*')<>NVL(o.%s,'*')))
            """.formatted(t.symbolCol(), t.nameCol(), t.table(), t.objectKey(), t.symbolCol(), t.nameCol());

        try( ResultSet rs = stmt.executeQuery(sql) ) {
            updateObjects(t.label(), rs);
        }
    }

    void updateObjects(String objType, ResultSet rs) throws Exception {

        log.info("");
        log.info("Starting update for "+objType);

        int namesChanged = 0;
        int symbolsChanged = 0;
        Set<Integer> objectsWithChangedNames = new HashSet<Integer>();
        Set<Integer> objectsWithChangedSymbols = new HashSet<Integer>();

        while (rs.next()) {
            String fullAnnotSymbol=rs.getString("OBJECT_SYMBOL");
            String objSymbol=rs.getString("OBJECT_SYMBOL2");
            String fullAnnotName=rs.getString("OBJECT_NAME");
            String objName=rs.getString("OBJECT_NAME2");
            int objRGDID=rs.getInt("ANNOTATED_OBJECT_RGD_ID");
            int fullAnnotKey=rs.getInt("FULL_ANNOT_KEY");

            boolean symbolChanged = !Utils.stringsAreEqual(fullAnnotSymbol, objSymbol);
            boolean nameChanged = !Utils.stringsAreEqual(fullAnnotName, objName);
            if( symbolChanged ) {
                log.debug("SYMBOL FASym=[" + fullAnnotSymbol+"] "+objType+"Sym=[" + objSymbol+"] FAKey="+fullAnnotKey+" RGDID=" + objRGDID);
                symbolsChanged++;
                objectsWithChangedSymbols.add(objRGDID);
            }
            if( nameChanged ) {
                log.debug("NAME FAName=[" + fullAnnotName+"] "+objType+"Name=[" + objName+"] FAKey="+fullAnnotKey+" RGDID=" + objRGDID);
                namesChanged++;
                objectsWithChangedNames.add(objRGDID);
            }

            if( symbolChanged || nameChanged ){
                update(objSymbol, objName, fullAnnotKey);
            }
        }

        log.info("    "+Utils.formatThousands(symbolsChanged) + " Symbol Updates for "+Utils.formatThousands(objectsWithChangedSymbols.size())+ " "+objType);
        log.info("    "+Utils.formatThousands(namesChanged) + " Name Updates for "+Utils.formatThousands(objectsWithChangedNames.size())+ " "+objType);
    }

    void update(String objectSymbol, String objectName, int fullAnnotKey) throws Exception {
        String sql = "UPDATE full_annot SET object_symbol=?,object_name=?,last_modified_date=SYSDATE,last_modified_by=? WHERE full_annot_key=?";
        dao.update(sql, objectSymbol, objectName, getLastModifiedBy(), fullAnnotKey);
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public int getLastModifiedBy() {
        return lastModifiedBy;
    }

    public void setLastModifiedBy(int lastModifiedBy) {
        this.lastModifiedBy = lastModifiedBy;
    }
}

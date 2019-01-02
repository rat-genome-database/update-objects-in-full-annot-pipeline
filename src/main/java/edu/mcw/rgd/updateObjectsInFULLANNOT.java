package edu.mcw.rgd;

import edu.mcw.rgd.dao.AbstractDAO;
import edu.mcw.rgd.process.Utils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

/**
 * Program to update OBJECT_SYMBOL(gene symbol) and OBJECT_NAME(gene name) in the FULL_ANNOT table
 * for all genes, strains, qtls and variants which have the same ANNOTATED_OBJECT_RGD_ID
 * with the RGD_ID field in the GENES table.
 */
public class updateObjectsInFULLANNOT {

    Logger log = Logger.getLogger("updates");
    AbstractDAO dao = new AbstractDAO();

    private String version;
    private int lastModifiedBy;

    public static void main(String[] args) throws Exception {

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        updateObjectsInFULLANNOT instance = (updateObjectsInFULLANNOT) (bf.getBean("manager"));

        try {
            instance.run();
        } catch(Exception e) {
            e.printStackTrace();
            Utils.printStackTrace(e, instance.log);
        }
    }

    public void run() throws Exception {

        long time0 = System.currentTimeMillis();

        log.info(getVersion());
        log.info("   "+dao.getConnectionInfo());
        log.info("=======");

        Statement stmt = dao.getConnection().createStatement();

        updateGenes(stmt);
        updateStrains(stmt);
        updateQtls(stmt);
        updateVariants(stmt);

        stmt.close();

        log.info("");
        log.info("=== OK ===  elapsed  "+Utils.formatElapsedTime(time0, System.currentTimeMillis()));
        log.info("");
    }

    void updateGenes(Statement stmt) throws Exception {

        ResultSet rs = stmt.executeQuery(
                "SELECT f.OBJECT_SYMBOL, f.OBJECT_NAME, f.ANNOTATED_OBJECT_RGD_ID, f.FULL_ANNOT_KEY, g.GENE_SYMBOL object_symbol2, g.FULL_NAME object_name2 "+
                "FROM FULL_ANNOT f, GENES g "+
                "WHERE f.RGD_OBJECT_KEY=1 AND f.ANNOTATED_OBJECT_RGD_ID = g.RGD_ID "+
                  "AND (NVL(f.OBJECT_SYMBOL,'*')<>NVL(g.GENE_SYMBOL,'*') OR (NVL(f.OBJECT_NAME,'*')<>NVL(g.FULL_NAME,'*')))");

        updateObjects("GENES", rs);

        rs.close();
    }

    void updateStrains(Statement stmt) throws Exception {

        ResultSet rs = stmt.executeQuery(
                "SELECT f.OBJECT_SYMBOL, f.OBJECT_NAME, f.ANNOTATED_OBJECT_RGD_ID, f.FULL_ANNOT_KEY, s.STRAIN_SYMBOL object_symbol2, s.FULL_NAME object_name2 "+
                "FROM FULL_ANNOT f, STRAINS s "+
                "WHERE f.RGD_OBJECT_KEY=5 AND f.ANNOTATED_OBJECT_RGD_ID = s.RGD_ID "+
                  "AND (NVL(f.OBJECT_SYMBOL,'*')<>NVL(s.STRAIN_SYMBOL,'*') OR (NVL(f.OBJECT_NAME,'*')<>NVL(s.FULL_NAME,'*')))");

        updateObjects("STRAINS", rs);

        rs.close();
    }

    void updateQtls(Statement stmt) throws Exception {

        ResultSet rs = stmt.executeQuery(
                "SELECT f.OBJECT_SYMBOL, f.OBJECT_NAME, f.ANNOTATED_OBJECT_RGD_ID, f.FULL_ANNOT_KEY, q.QTL_SYMBOL object_symbol2, q.QTL_NAME object_name2 "+
                "FROM FULL_ANNOT f, QTLS q "+
                "WHERE f.RGD_OBJECT_KEY=6 AND f.ANNOTATED_OBJECT_RGD_ID = q.RGD_ID "+
                  "AND (NVL(f.OBJECT_SYMBOL,'*')<>NVL(q.QTL_SYMBOL,'*') OR (NVL(f.OBJECT_NAME,'*')<>NVL(q.QTL_NAME,'*')))");

        updateObjects("QTLS", rs);

        rs.close();
    }

    void updateVariants(Statement stmt) throws Exception {

        ResultSet rs = stmt.executeQuery(
                "SELECT f.OBJECT_SYMBOL, f.OBJECT_NAME, f.ANNOTATED_OBJECT_RGD_ID, f.FULL_ANNOT_KEY, g.SYMBOL object_symbol2, g.NAME object_name2 "+
                "FROM FULL_ANNOT f, GENOMIC_ELEMENTS g "+
                "WHERE f.RGD_OBJECT_KEY=7 AND f.ANNOTATED_OBJECT_RGD_ID = g.RGD_ID "+
                  "AND (NVL(f.OBJECT_SYMBOL,'*')<>NVL(g.SYMBOL,'*') OR (NVL(f.OBJECT_NAME,'*')<>NVL(g.NAME,'*')))");

        updateObjects("VARIANTS", rs);

        rs.close();
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

        log.info("=======");
        log.info(symbolsChanged + " Symbol Updates for "+objectsWithChangedSymbols.size()+ " "+objType);
        log.info(namesChanged + " Name Updates for "+objectsWithChangedNames.size()+ " "+objType);
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

package edu.mcw.rgd;
import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.process.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

/**
 * Program to update OBJECT_SYMBOL(gene symbol) and OBJECT_NAME(gene name) in the FULL_ANNOT table
 * for all genes, strains, qtls and variants which have the same ANNOTATED_OBJECT_RGD_ID
 * with the RGD_ID field in the GENES table.
 */
public class updateObjectsInFULLANNOT {

    Log log = LogFactory.getLog("updates");

    public static void main(String[] args) throws Exception {

        updateObjectsInFULLANNOT instance = new updateObjectsInFULLANNOT();

        try {
            instance.run();
        } catch(Exception e) {
            e.printStackTrace();
            Utils.printStackTrace(e, instance.log);
        }
    }

    public void run() throws Exception {

        Connection connection = DataSourceFactory.getInstance().getDataSource().getConnection();
        log.info("updateGeneNamesInFULLANNOT v. 1.2.3, built on May 2, 2017");

        Statement stmt = connection.createStatement(
                                      ResultSet.TYPE_SCROLL_INSENSITIVE,
                                      ResultSet.CONCUR_UPDATABLE);
        String updateQuery = "UPDATE FULL_ANNOT SET OBJECT_SYMBOL=?,OBJECT_NAME=?,LAST_MODIFIED_DATE=SYSDATE,LAST_MODIFIED_BY=170 WHERE FULL_ANNOT_KEY=?";
        PreparedStatement psUpdate = connection.prepareStatement(updateQuery);

        updateGenes(stmt, psUpdate);
        updateStrains(stmt, psUpdate);
        updateQtls(stmt, psUpdate);
        updateVariants(stmt, psUpdate);

        connection.close();
    }

    void updateGenes(Statement stmt, PreparedStatement psUpdate) throws Exception {

        ResultSet rs = stmt.executeQuery(
                "SELECT f.OBJECT_SYMBOL, f.OBJECT_NAME, f.ANNOTATED_OBJECT_RGD_ID, f.FULL_ANNOT_KEY, g.GENE_SYMBOL object_symbol2, g.FULL_NAME object_name2 "+
                "FROM FULL_ANNOT f, GENES g "+
                "WHERE f.RGD_OBJECT_KEY=1 AND f.ANNOTATED_OBJECT_RGD_ID = g.RGD_ID "+
                  "AND (NVL(f.OBJECT_SYMBOL,'*')<>NVL(g.GENE_SYMBOL,'*') OR (NVL(f.OBJECT_NAME,'*')<>NVL(g.FULL_NAME,'*')))");

        updateObjects("GENES", rs, psUpdate);

        rs.close();
    }

    void updateStrains(Statement stmt, PreparedStatement psUpdate) throws Exception {

        ResultSet rs = stmt.executeQuery(
                "SELECT f.OBJECT_SYMBOL, f.OBJECT_NAME, f.ANNOTATED_OBJECT_RGD_ID, f.FULL_ANNOT_KEY, s.STRAIN_SYMBOL object_symbol2, s.FULL_NAME object_name2 "+
                "FROM FULL_ANNOT f, STRAINS s "+
                "WHERE f.RGD_OBJECT_KEY=5 AND f.ANNOTATED_OBJECT_RGD_ID = s.RGD_ID "+
                  "AND (NVL(f.OBJECT_SYMBOL,'*')<>NVL(s.STRAIN_SYMBOL,'*') OR (NVL(f.OBJECT_NAME,'*')<>NVL(s.FULL_NAME,'*')))");

        updateObjects("STRAINS", rs, psUpdate);

        rs.close();
    }

    void updateQtls(Statement stmt, PreparedStatement psUpdate) throws Exception {

        ResultSet rs = stmt.executeQuery(
                "SELECT f.OBJECT_SYMBOL, f.OBJECT_NAME, f.ANNOTATED_OBJECT_RGD_ID, f.FULL_ANNOT_KEY, q.QTL_SYMBOL object_symbol2, q.QTL_NAME object_name2 "+
                "FROM FULL_ANNOT f, QTLS q "+
                "WHERE f.RGD_OBJECT_KEY=6 AND f.ANNOTATED_OBJECT_RGD_ID = q.RGD_ID "+
                  "AND (NVL(f.OBJECT_SYMBOL,'*')<>NVL(q.QTL_SYMBOL,'*') OR (NVL(f.OBJECT_NAME,'*')<>NVL(q.QTL_NAME,'*')))");

        updateObjects("QTLS", rs, psUpdate);

        rs.close();
    }

    void updateVariants(Statement stmt, PreparedStatement psUpdate) throws Exception {

        ResultSet rs = stmt.executeQuery(
                "SELECT f.OBJECT_SYMBOL, f.OBJECT_NAME, f.ANNOTATED_OBJECT_RGD_ID, f.FULL_ANNOT_KEY, g.SYMBOL object_symbol2, g.NAME object_name2 "+
                "FROM FULL_ANNOT f, GENOMIC_ELEMENTS g "+
                "WHERE f.RGD_OBJECT_KEY=7 AND f.ANNOTATED_OBJECT_RGD_ID = g.RGD_ID "+
                  "AND (NVL(f.OBJECT_SYMBOL,'*')<>NVL(g.SYMBOL,'*') OR (NVL(f.OBJECT_NAME,'*')<>NVL(g.NAME,'*')))");

        updateObjects("VARIANTS", rs, psUpdate);

        rs.close();
    }

    void updateObjects(String objType, ResultSet rs, PreparedStatement psUpdate) throws Exception {

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
                psUpdate.setString(1, objSymbol);
                psUpdate.setString(2, objName);
                psUpdate.setInt(3, fullAnnotKey);
                psUpdate.executeUpdate();
            }
        }

        log.info("=======");
        log.info(symbolsChanged + " Symbol Updates for "+objectsWithChangedSymbols.size()+ " "+objType);
        log.info(namesChanged + " Name Updates for "+objectsWithChangedNames.size()+ " "+objType);
    }
}

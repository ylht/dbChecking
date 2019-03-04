package ecnu.db.work.group;

import ecnu.db.work.CheckType;
import ecnu.db.scheme.Table;

import java.util.ArrayList;
import java.util.Collections;

public class InitAllWorkGroup {
    private ArrayList<BaseWorkGroup> workGroups = new ArrayList<>();

    public InitAllWorkGroup(Table[] tables, CheckType checkType) {

        ArrayList<WorkNode> workNodes = getWorkNodes(tables);
        if(checkType.isCheckPhantomRead()){
            BaseWorkGroup phantomReadWorkGroup=new PhantomReadWorkGroup(true);
            phantomReadWorkGroup.addInTuple(getWorkNode(workNodes.remove(0), tables));
            workGroups.add(phantomReadWorkGroup);
        }
        Collections.shuffle(workNodes);
        int i=0;
        if (checkType.isCheckWriteSkew()) {
            //writeSkew类型的工作组，只有测试Serializable时，才测试此事务
            BaseWorkGroup writeSkewWorkGroup = new WriteSkewWorkGroup();
            writeSkewWorkGroup.addOutTuple(getWorkNode(workNodes.remove(0), tables));
            for (WorkNode workNode : workNodes) {
                if (workNode.getTableIndex() == writeSkewWorkGroup.getOut().get(0).getTableIndex()) {
                    writeSkewWorkGroup.addOutTuple(getWorkNode(workNode, tables));
                    workNodes.remove(workNode);
                    break;
                }
            }
            workGroups.add(writeSkewWorkGroup);
        }

        //function类型的工作组
        BaseWorkGroup functionWorkGroup = new FunctionWorkGroup();
        functionWorkGroup.addInTuple(getWorkNode(workNodes.get(i++), tables));
        functionWorkGroup.addOutTuple(getWorkNode(workNodes.get(i++), tables));
        workGroups.add(functionWorkGroup);

        //order类型的工作组
        BaseWorkGroup orderWorkGroup = new OrderWorkGroup();
        orderWorkGroup.addInTuple(getWorkNode(workNodes.get(i++), tables));
        workGroups.add(orderWorkGroup);

        //remittance类型的工作组
        //1.转账给自身
        BaseWorkGroup remittanceToItselfWorkGroup = new RemittanceWorkGroup();
        remittanceToItselfWorkGroup.addInoutTuple(getWorkNode(workNodes.get(i++), tables));
        workGroups.add(remittanceToItselfWorkGroup);

        //2.两列之间互相转账
        BaseWorkGroup remittanceEachOtherWorkGroup = new RemittanceWorkGroup();
        remittanceEachOtherWorkGroup.addInTuple(getWorkNode(workNodes.get(i++), tables));
        remittanceEachOtherWorkGroup.addOutTuple(getWorkNode(workNodes.get(i++), tables));
        workGroups.add(remittanceEachOtherWorkGroup);

        ArrayList<WorkNode> valueChangeNodes=new ArrayList<>(workNodes.subList(0,i-1));
        Collections.shuffle(valueChangeNodes);

        if(checkType.isCheckRepeatableRead()){
            BaseWorkGroup repeatableRead=new RepeatableReadWorkGroup();
            repeatableRead.addInTuple(getWorkNode(valueChangeNodes.get(0),tables));
            workGroups.add(repeatableRead);
            Collections.shuffle(workNodes);
        }

        if(checkType.isCheckNoCommitted()){
            BaseWorkGroup checkNoCommit=new NoCommitWorkGroup();
            checkNoCommit.addInTuple(getWorkNode(valueChangeNodes.get(0),tables));
            workGroups.add(checkNoCommit);
            Collections.shuffle(workNodes);
        }

        if(checkType.isCheckPhantomRead()){
            BaseWorkGroup phantomReadWorkGroup=new PhantomReadWorkGroup(false);
            phantomReadWorkGroup.addInTuple(getWorkNode(workNodes.get(0), tables));
            workGroups.add(phantomReadWorkGroup);
        }
    }

    public ArrayList<BaseWorkGroup> getWorkGroups() {
        return workGroups;
    }

    private ArrayList<WorkNode> getWorkNodes(Table[] tables) {
        ArrayList<WorkNode> workNodes = new ArrayList<>();
        int i = 0;
        for (Table table : tables) {
            for (int j = 1; j < table.getTableColSizeExceptKey() + 1; j++) {
                workNodes.add(new WorkNode(i, j));
            }
            i++;
        }
        return workNodes;
    }

    private WorkNode getWorkNode(WorkNode workNode, Table[] tables) {
        workNode.setAddValueList(tables[workNode.getTableIndex()].getKeys());
        workNode.setSubValueList(tables[workNode.getTableIndex()].getKeys());
        return workNode;
    }
}

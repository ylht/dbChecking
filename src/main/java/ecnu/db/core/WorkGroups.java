package ecnu.db.core;

import ecnu.db.scheme.Table;

import java.util.ArrayList;
import java.util.Collections;

public class WorkGroups {
    private ArrayList<WorkGroup> workGroups = new ArrayList<>();

    public WorkGroups(Table[] tables, CheckType.CheckKind checkKind) {
        /*
          workGroup分类细则
          一、如果inout不为空，那么必定为转账业务测试，其他两种类型的测试不涉及到同列数据的操作
          二、如果inout为空
            1. 如果out为空，为order型业务
            2. 如果in为空，为writeSkew型事务
            3. 如果in和out的size都为1可以判定为in=k*out+b型的数据
            4. 如果in和out不为空，且有一个的size大于1，那么可以认定为还是转账业务
         */
        ArrayList<WorkNode> workNodes = getWorkNodes(tables);
        Collections.shuffle(workNodes);

        //writeSkew类型的工作组，只有测试Serializable时，才测试此事务
        if (checkKind == CheckType.CheckKind.Serializable) {
            WorkGroup writeSkewWorkGroup = new WorkGroup(WorkGroup.WorkGroupType.writeSkew);
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
        WorkGroup functionWorkGroup = new WorkGroup(WorkGroup.WorkGroupType.function);
        functionWorkGroup.addInTuple(getWorkNode(workNodes.remove(0), tables));
        functionWorkGroup.addOutTuple(getWorkNode(workNodes.remove(0), tables));
        workGroups.add(functionWorkGroup);

        //order类型的工作组
        WorkGroup orderWorkGroup = new WorkGroup(WorkGroup.WorkGroupType.order);
        orderWorkGroup.addInTuple(getWorkNode(workNodes.remove(0), tables));
        workGroups.add(orderWorkGroup);

        //remittance类型的工作组
        //1.转账给自身
        WorkGroup remittanceToItselfWorkGroup = new WorkGroup(WorkGroup.WorkGroupType.remittance);
        remittanceToItselfWorkGroup.addInoutTuple(getWorkNode(workNodes.remove(0), tables));
        workGroups.add(remittanceToItselfWorkGroup);

        //2.两列之间互相转账
        WorkGroup remittanceEachOtherWorkGroup = new WorkGroup(WorkGroup.WorkGroupType.remittance);
        remittanceEachOtherWorkGroup.addInTuple(getWorkNode(workNodes.remove(0), tables));
        remittanceEachOtherWorkGroup.addOutTuple(getWorkNode(workNodes.remove(0), tables));
        workGroups.add(remittanceEachOtherWorkGroup);

    }

    public ArrayList<WorkGroup> getWorkGroups() {
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

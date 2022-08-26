package ch.usi.dslab.lel.dynastar.tpcc;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.lel.dynastar.tpcc.command.Command;
import ch.usi.dslab.lel.dynastar.tpcc.command.CommandType;
import ch.usi.dslab.lel.dynastar.tpcc.objects.ObjId;

/**
 * Author: longle, created on 07/03/16.
 */
public class TpccCommand {
    Map<String, Object> attributes;
    private Command command;
    private CommandType commandType;
    private ObjId objId;

    public TpccCommand(CommandType type, ObjId objId, Object... params) {
        this.commandType = type;
        this.objId = objId;
        attributes = new HashMap<>();
        for (int i = 0; i < params.length; ++i) {
            attributes.put(String.valueOf(params[i]), params[++i]);
        }
    }

    public TpccCommand(CommandType type, ObjId objId, Map<String, Object> attributes) {
        this.commandType = type;
        this.attributes = attributes;
        this.objId = objId;
    }

//    public Command getCommand() {
//        if (commandType instanceof GenericCommand) {
//            switch ((GenericCommand) commandType) {
//                case CREATE:
//                    DSqlCommandPayload payload = new DSqlCommandPayload(attributes);
//                    command = new Command(commandType, objId, payload);
//                    break;
//                case READ_BATCH:
//                    command = new Command(commandType, objId);
//                    command.setRangeCommand(true);
//                    break;
//                case READ:
//                case DELETE:
//                    command = new Command(commandType, objId);
//                    break;
//            }
//        } else {
//            DSqlCommandPayload payload;
//            switch ((DSqlCommandType) commandType) {
//                case UPDATE:
//                    payload = new DSqlCommandPayload(attributes);
//                    command = new Command(commandType, objId, payload);
//                    break;
//                case UPDATE_BATCH:
//                    payload = new DSqlCommandPayload(attributes);
//                    command = new Command(commandType, objId, payload);
//                    command.setRangeCommand(true);
//                    break;
//            }
//        }
//        return command;
//    }
//
//    public CompletableFuture<Message> execute(PromiseClient aerieAsyncClient) {
//        Command command = getCommand();
//        if (commandType instanceof GenericCommand) {
//            switch ((GenericCommand) commandType) {
//                case CREATE:
//                    return aerieAsyncClient.create(command);
//                case READ:
//                    return aerieAsyncClient.read(command);
//                case READ_BATCH:
//                    return aerieAsyncClient.readBatch(command);
//                case DELETE:
//                    return aerieAsyncClient.delete(command);
//
//            }
//        } else {
//            switch ((DSqlCommandType) commandType) {
//                case UPDATE:
//                    return aerieAsyncClient.update(command);
//                case UPDATE_BATCH:
//                    return aerieAsyncClient.updateBatch(command);
//            }
//        }
//        return null;
//    }
//
//    public enum DSqlCommandType implements CommandType {
//        UPDATE, UPDATE_BATCH, READ_SSMR
//    }

}

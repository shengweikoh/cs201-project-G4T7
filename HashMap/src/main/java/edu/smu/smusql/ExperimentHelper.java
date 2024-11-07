package edu.smu.smusql;

public class ExperimentHelper {
    public String generateSelectCommand() {
        return "SELECT * FROM table_name WHERE column = value";
    }

    public String generateInsertCommand() {
        return "INSERT INTO table_name VALUES (value1, value2, ...)";
    }

    public String generateUpdateCommand() {
        return "UPDATE table_name SET column = newValue WHERE column = condition";
    }

    public String generateDeleteCommand() {
        return "DELETE FROM table_name WHERE column = condition";
    }

    public void executeWithRatio(int numCommands, double selectRatio, double insertRatio, double updateRatio, double deleteRatio, Engine engine) {
        int selectCount = (int) (numCommands * selectRatio);
        int insertCount = (int) (numCommands * insertRatio);
        int updateCount = (int) (numCommands * updateRatio);
        int deleteCount = (int) (numCommands * deleteRatio);

        for (int i = 0; i < numCommands; i++) {
            String command;
            if (i < selectCount) {
                command = generateSelectCommand();
            } else if (i < selectCount + insertCount) {
                command = generateInsertCommand();
            } else if (i < selectCount + insertCount + updateCount) {
                command = generateUpdateCommand();
            } else {
                command = generateDeleteCommand();
            }
            engine.executeSQL(command);
        }
    }
}

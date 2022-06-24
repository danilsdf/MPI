import mpi.*;


public class lab6 {
    final static int MASTER = 0;
    final static int FROM_MASTER = 1;
    final static int FROM_WORKER = 2;

    public static void main(String[] args) {
        int n = 10;
        MPI.Init(args);
        printActions();
        Blocking(true, n);
        MPI.Finalize();
        NonBlocking(true, n);
    }

    public static void printActions(){
        int num_tasks;
        int task_id;
        String hostname;
        num_tasks = MPI.COMM_WORLD.Size();
        task_id = MPI.COMM_WORLD.Rank();
        hostname = MPI.Get_processor_name();
        System.out.println("Hello from number task  " + task_id + " on host " + hostname + " !\n" );
        if (task_id == MASTER)
            System.out.println("Number of MPI tasks is: " + num_tasks);
    }

    public static void Blocking(boolean printResult, int size) {

        double[][] a = new double[size][size];
        double[][] b = new double[size][size];
        double[][] c = new double[size][size];
        int rows;
        int offset = 0;
        int[] _offset = new int[1];
        int[] _rows = new int[1];
        double startTime;
        double totalTime;
        final int rank = MPI.COMM_WORLD.Rank();
        final int num_tasks = MPI.COMM_WORLD.Size();
        int num_workers = num_tasks - 1;
        if (rank == MASTER) {
            for (int i=0; i<size; i++){
                for (int j=0; j<size; j++){
                    a[i][j]= j+1;
                }
            }
            for (int i=0; i<size; i++){
                for (int j=0; j<size; j++) {
                    b[i][j] = (i==j) ? 1 : 0;
                }
            }
            int x = size/ num_workers;
            int y = size% num_workers;
            startTime = System.currentTimeMillis();
            for (int dest = 1; dest<= num_workers; dest++) {
                rows = (dest <= y) ? x + 1 : x;
                _offset[0] = offset;
                _rows[0] = rows;
                double[][] a_buf = new double[rows][size];
                System.arraycopy(a, offset, a_buf, 0, rows);
                MPI.COMM_WORLD.Send(_offset, 0, 1,MPI.INT, dest, FROM_MASTER);
                MPI.COMM_WORLD.Send(_rows, 0, 1, MPI.INT, dest, FROM_MASTER);
                MPI.COMM_WORLD.Send(a_buf, 0, rows, MPI.OBJECT, dest, FROM_MASTER);
                MPI.COMM_WORLD.Send(b, 0,size, MPI.OBJECT, dest, FROM_MASTER);
                offset += rows;
            }
            for (int source = 1; source<= num_workers; source++) {
                MPI.COMM_WORLD.Recv(_offset, 0, 1, MPI.INT, source, FROM_WORKER);
                MPI.COMM_WORLD.Recv(_rows, 0, 1, MPI.INT, source, FROM_WORKER);
                MPI.COMM_WORLD.Recv(c, _offset[0],_rows[0], MPI.OBJECT, source, FROM_WORKER);
            }
            totalTime = System.currentTimeMillis() - startTime;
            System.out.println(totalTime);
            if (printResult)
                printResult(c, size);}
        else {
            MPI.COMM_WORLD.Recv(_offset, 0,1, MPI.INT, MASTER, FROM_MASTER);
            MPI.COMM_WORLD.Recv(_rows, 0,1, MPI.INT, MASTER, FROM_MASTER);
            a = new double[_rows[0]][size];
            MPI.COMM_WORLD.Recv(a, 0, _rows[0], MPI.OBJECT, MASTER, FROM_MASTER);
            MPI.COMM_WORLD.Recv(b, 0, size, MPI.OBJECT, MASTER, FROM_MASTER);
            for (int k=0; k<size; k++) {
                for (int i = 0; i < _rows[0]; i++) {
                    c[i][k] = 0.0;
                    for (int j = 0; j < size; j++) {
                        c[i][k] += a[i][j] * b[j][k];
                    }
                }
            }
            MPI.COMM_WORLD.Send(_offset, 0,1, MPI.INT, MASTER, FROM_WORKER);
            MPI.COMM_WORLD.Send(_rows, 0,1, MPI.INT, MASTER, FROM_WORKER);
            MPI.COMM_WORLD.Send(c, 0,_rows[0], MPI.OBJECT, MASTER, FROM_WORKER);
        }
    }

    public static void NonBlocking(boolean printResult, int size) {
        double[][] a = new double[size][size];
        double[][] b = new double[size][size];
        double[][] c = new double[size][size];
        int[] _offset = new int[1];
        int[] _rows = new int[1];
        int offset = 0;
        double startTime;
        double totalTime;
        int rows;
        final int rank = MPI.COMM_WORLD.Rank();
        final int num_tasks = MPI.COMM_WORLD.Size();

        int num_workers = num_tasks -1;
        if (rank == MASTER) {
            for (int i=0; i<size; i++){
                for (int j=0; j<size; j++){
                    a[i][j]= 10;
                }
            }
            for (int i=0; i<size; i++){
                for (int j=0; j<size; j++) {
                    b[i][j] = (i == j) ? 1 : 0;
                }
            }
            int x = size/ num_workers;
            int y = size% num_workers;
            startTime = System.currentTimeMillis();
            for (int dest = 1; dest<= num_workers; dest++) {
                rows = (dest <= y) ? x+1 : x;
                _offset[0] = offset;
                _rows[0] = rows;
                double[][] a_buf = new double[rows][size];
                System.arraycopy(a, offset, a_buf, 0, rows);
                Request[] sendRequest = new Request[4];
                sendRequest[0] = MPI.COMM_WORLD.Isend(_offset, 0, 1,MPI.INT, dest, FROM_MASTER);
                sendRequest[1] = MPI.COMM_WORLD.Isend(_rows, 0, 1, MPI.INT, dest, FROM_MASTER);
                sendRequest[2] = MPI.COMM_WORLD.Isend(a_buf, 0, rows, MPI.OBJECT, dest, FROM_MASTER);
                sendRequest[3] = MPI.COMM_WORLD.Isend(b, 0, size, MPI.OBJECT, dest, FROM_MASTER);
                Request.Waitall(sendRequest);
                offset = offset + rows;
            }
            Request[] receiveRequest = new Request[num_workers];
            for (int source = 1; source<= num_workers; source++) {
                MPI.COMM_WORLD.Recv(_offset, 0,1, MPI.INT, source, FROM_WORKER);
                MPI.COMM_WORLD.Recv(_rows, 0,1, MPI.INT, source, FROM_WORKER);
                receiveRequest[source-1] = MPI.COMM_WORLD.Irecv(c, _offset[0],_rows[0], MPI.OBJECT, source, FROM_WORKER);
                System.out.println("Received results from task %d\n" +  source);
            }
            Request.Waitall(receiveRequest);
            totalTime = System.currentTimeMillis()-startTime;
            System.out.println(totalTime);
            if (printResult)
                printResult(c, size);}
        else {
            MPI.COMM_WORLD.Recv(_offset, 0,1, MPI.INT, MASTER, FROM_MASTER);
            MPI.COMM_WORLD.Recv(_rows, 0,1, MPI.INT, MASTER, FROM_MASTER);
            a = new double[_rows[0]][size];
            MPI.COMM_WORLD.Recv(a, 0, _rows[0], MPI.OBJECT, MASTER, FROM_MASTER);
            MPI.COMM_WORLD.Recv(b, 0, size, MPI.OBJECT, MASTER, FROM_MASTER);
            for (int k=0; k<size; k++) {
                for (int i = 0; i < _rows[0]; i++) {
                    c[i][k] = 0.0;
                    for (int j = 0; j < size; j++) {
                        c[i][k] += a[i][j] * b[j][k];
                    }
                }
            }
            MPI.COMM_WORLD.Send(_offset, 0,1, MPI.INT, MASTER, FROM_WORKER);
            MPI.COMM_WORLD.Send(_rows, 0,1, MPI.INT, MASTER, FROM_WORKER);
            MPI.COMM_WORLD.Send(c, 0,_rows[0], MPI.OBJECT, MASTER, FROM_WORKER);
        }
    }


    public static void printResult(double[][] c, int size){
        System.out.println("Result Matrix:\n");
        for (int k = 0; k < size; k++)
        {
            System.out.println("\n");
            for (int t = 0; t < size; t++)
                System.out.printf("%6.2f ", c[k][t]);
        }
    }
}

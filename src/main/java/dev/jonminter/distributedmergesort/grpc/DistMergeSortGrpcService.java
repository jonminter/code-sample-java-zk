package dev.jonminter.distributedmergesort.grpc;

import dev.jonminter.distributedmergesort.DistMergeSortContext;
import dev.jonminter.distributedmergesort.employees.output.EmployeeWithDepartmentAndManager;
import dev.jonminter.distributedmergesort.server.DistributedMergeSortGrpc;
import dev.jonminter.distributedmergesort.server.Employee;
import dev.jonminter.distributedmergesort.server.NextBatchOfSortedEmployeesRequest;
import dev.jonminter.distributedmergesort.server.NextBatchOfSortedEmployeesResponse;
import dev.jonminter.distributedmergesort.server.NotifyFollowersOfLeaderRequest;
import dev.jonminter.distributedmergesort.server.NotifyFollowersOfLeaderResponse;
import io.grpc.stub.StreamObserver;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DistMergeSortGrpcService extends DistributedMergeSortGrpc.DistributedMergeSortImplBase {
  private static final Logger logger = Logger.getLogger(DistMergeSortGrpcService.class.getName());

  private final DistMergeSortContext context;

  public DistMergeSortGrpcService(DistMergeSortContext context) {
    this.context = context;
  }

  @Override
  public void notifyFollowerOfLeader(NotifyFollowersOfLeaderRequest request,
      StreamObserver<NotifyFollowersOfLeaderResponse> responseObserver) {
    logger.log(Level.INFO, "We are following leader {0}", request.getLeaderNode());

    context.startDistMergeSort();

    responseObserver.onNext(NotifyFollowersOfLeaderResponse
        .newBuilder()
        .setAck(true)
        .build());
    responseObserver.onCompleted();
  }

  @Override
  public void nextBatchOfSortedEmployees(NextBatchOfSortedEmployeesRequest request, StreamObserver<Employee> responseObserver) {
    logger.log(Level.INFO, "Fetching next batch of {0} sorted employees!", request.getBatchSize());

    int leftToRetrieve = request.getBatchSize();
    NextBatchOfSortedEmployeesResponse.Builder responseBuilder = NextBatchOfSortedEmployeesResponse
        .newBuilder();
    PriorityQueue<EmployeeWithDepartmentAndManager> sortedEmployees = context.getSortedEmployees();

    int i = 0;
    while (leftToRetrieve > 0 && sortedEmployees.size() > 0) {
      EmployeeWithDepartmentAndManager nextEmployee = sortedEmployees.poll();
      responseObserver.onNext(EmployeeTransformer.fromEmployeeWithManagerAmdDepartment(nextEmployee));
      i++;
      leftToRetrieve--;
    }

    logger.log(Level.INFO, "Responded with top {0} employee records", i);

    responseObserver.onCompleted();
  }
}

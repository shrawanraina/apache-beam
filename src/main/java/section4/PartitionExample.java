package section4;

import org.apache.beam.sdk.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.Partition.*;
import org.apache.beam.sdk.values.*;
import org.checkerframework.checker.initialization.qual.*;
import org.checkerframework.checker.nullness.qual.*;

public class PartitionExample {

  // A: > 90
  // B: 80 - 89
  // C: 70 - 79
  // D: 60 - 69
  // F: < 60


  static final Integer NUMBER_OF_PARTITIONS = 5;

  public static void main(String[] args) {
    Pipeline p = Pipeline.create();
    PCollection<Integer> marks = p.apply(Create.of(45, 88, 91, 72, 56, 43, 96));
    PCollectionList<Integer> marksByGrade = marks.apply(
        Partition.of(NUMBER_OF_PARTITIONS, new PartitionFn<Integer>() {
          @Override
          public @UnknownKeyFor @NonNull @Initialized int partitionFor(Integer elem,
              @UnknownKeyFor @NonNull @Initialized int numPartitions) {
            if (elem > 90) {
              return 0;
            } else if (elem >= 80 && elem <= 89) {
              return 1;
            } else if (elem >= 70 && elem <= 79) {
              return 2;
            } else if (elem >= 60 && elem <= 69) {
              return 3;
            }
            return 4;
          }
        }));
    PCollection<Integer> gradeA = marksByGrade.get(0);
    gradeA.apply(ParDo.of(new PrintElementFn()));
    // PCollection<Integer> gradeB = marksByGrade.get(1);
    // gradeB.apply(ParDo.of(new PrintElementFn()));
    // PCollection<Integer> gradeC = marksByGrade.get(2);
    // gradeC.apply(ParDo.of(new PrintElementFn()));
    // PCollection<Integer> gradeD = marksByGrade.get(3);
    // gradeD.apply(ParDo.of(new PrintElementFn()));
    // PCollection<Integer> gradeF = marksByGrade.get(4);
    // gradeF.apply(ParDo.of(new PrintElementFn()));
    p.run().waitUntilFinish();
  }
}

# -*- coding: utf-8 -*-
"""
Output tracking pipeline wrapper for fyrd.

The fundamental unit of a fyrd pipeline is the Step. A Step is a thin wrapper
around a fyrd Job that explicitly defines inputs and ouputs (either actual
input/output files or pickled function outputs) and does not delete those
files (unless temp is explicitly passed). This is in contrast to a fyrd Job,
which defaults to auto-delete outputs once they are fetched.

This allows persistent dependency tracking and avoids rerunning jobs, which is
important in a pipeline setting.

At it's simplest, this allows the following type of thing:

..code :: python

    # Simple shell script with file outputs
    STEP1 = Step(
        'samtools sort -o {output} {input}', inputs='mybam.bam',
        outputs='sortedbam.bam', name='sort_bam'
    )
    # Multiple inputs, get output from STEP1, output is function output
    STEP2 = Step(
        filter_function, args=('{input.bam}', '{input.snps}', 24),
        inputs={'bam': STEP1.outputs, 'snps': 'mysnps'}, name='filter_bam'
    )
    # Will run in parallel to STEP2, args default to all outputs from STEP1
    # unless overriden by args
    STEP3 = Step(
        analyze_depth, inputs=STEP1, name='check_depth'
    )
    # Provides an incremental output file—ie. file beins at start of execution
    # and is written continually
    STEP4 = Step(
        'custom_counter {input} > {output[1]}', inputs='mybam.bam',
        outputs=(incremental('mybam.filter.bam'), 'stats.txt')
    )
    # Works on the above incremental file
    STEP5 = Step(
        'summarize_counts {input} > {output}', inputs=STEP4[0],
        outputs='final_summary.txt'
    )
    # Work on same file in parallel, but wait until completion, output is
    # function out
    STEP6 = Step(
        parse_counts, inputs=skip_incremental(STEP4[0])
    )
    # Combine data, inputs converted to keyword args
    STEP7 = Step(
        combine, inputs={'filtered': STEP1, 'counts': STEP6}
    )
    # Will run STEP1, STEP4 first, then STEP2, STEP3 after STEP1 finishes and
    # STEP5, STEP6 after STEP4 finishes (in parallel to other jobs still
    # running), and STEP7 after everything else is done. If a step fails,
    # previous successful steps are not repeated on rerun
    final_out = STEP7.get()

To combine into a reusable pipeline, just replace 'mybam.bam' input with
``{pipeline.input[0]}`` and 'mysnps' with ``{pipeline.input[1]}`` and run:

..code :: python

    pipeline = Pipeline(
        steps=[STEP1, STEP2, STEP3, STEP4, STEP5, STEP6, STEP7]
    )
    final_out = pipeline.get('mybam.bam', 'mysnps')
    steps = []
    for i in [i for i in os.listdir('.') if i.endswith('bam')]:
        steps = pipeline.submit(i, 'mysnps')
    outputs = get_pipeline_outputs(steps)
"""
pass

REF=ref
QUEUE=short
WORKERS=40
CORES=5
CMEM=8
N=7
M=5

SWORKERS=10
SMEM=40

export PATH:=${PATH}

spark_run = WORKERS=${WORKERS} CORES=${CORES} CMEM=${CMEM} QUEUE=${QUEUE} source run-saspark.sh; \
srun -x c0178,c3085 --time 1-0 -p ${QUEUE} -N 1 -c 2 --mem=40G -d "after:$$SPARK_MASTER_JOBID" bash -c "$(1)"; \
scancel $$SPARK_WORKER_JOBID $$SPARK_MASTER_JOBID

slurm_run = srun --time 1-0 -p ${QUEUE} -N 1 -c $(SWORKERS) --mem=$(SMEM)G bash -c "$(1)";

gpu_run = srun -p gpu --gres=gpu:k80:1 -N 1 -c 1 --mem=4G --time=08:00:00 bash -c "$(1)";

segment=../arabic_script_ocr_models/pretrained_print_layout.mlmodel
ocr=print_transcription_NEW.mlmodel

.SECONDARY:

eq := =
comma := ,

%/dataset.arrow:	%/alto.lis
	$(call slurm_run,ketos compile --workers $(SWORKERS) --ignore-splits --random-split 0.9 0.1 0.0 -o $@ -f alto -F $<)

%/alto-nall/alto.lis:	%/lines.json/_SUCCESS
	$(call slurm_run,vtrun patch-alto.py $(dir $<) $(dir $@))
	find $(dir $@) -type f -name '*.xml' > $@

%/alto-n1000/alto.lis:	%/lines.json/_SUCCESS
	$(call slurm_run,vtrun patch-alto.py -n 1000 $(dir $<) $(dir $@))
	find $(dir $@) -type f -name '*.xml' > $@

%/lines.json/_SUCCESS:	%/out.json/_SUCCESS
	$(call slurm_run,vtrun wit-lines.py $(dir $<) $(dir $@))

%-n$(N)m$(M).out/out.json/_SUCCESS:	%.in/_SUCCESS
	$(call spark_run,seriatim --docwise --log-level INFO --floating-ngrams --complete-lines -n $(N) -m $(M) --fields ref --filterpairs 'ref = 1 AND ref2 = 0' $(dir $<) $(dir $(patsubst %/,%,$(dir $@))))

images/_SUCCESS:	$(patsubst %,images/%/_SUCCESS,$(basename $(notdir $(wildcard pdf/*))))
	touch $@

images/%/_SUCCESS:	pdf/%.pdf
	(mkdir -p images/$*; cd images/$*; $(call slurm_run,pdf_images.py ../../pdf/$*.pdf)) && touch $@

splits/_SUCCESS:	images/_SUCCESS
	mkdir -p $(dir $@)
	find images -type f -name \*.png | split -l 100 - $(dir $@)x
	touch $@

print/_SUCCESS:	splits/_SUCCESS
	$(MAKE) SMEM=10 $(patsubst splits/%,print/jobs/_%,$(wildcard splits/x*))
	touch $@

print/jobs/_%:	splits/%
	mkdir -p $(dir $@)
	$(call slurm_run,cat $< | ./batkraken.sh print --alto segment --model $(segment) -d horizontal-rl --baseline ocr --model $(ocr) --base-dir R >& $@.err)
	touch $@

gen1-print/_SUCCESS:	splits/_SUCCESS
	$(MAKE) ocr=print-n7m5.out/alto-nall/ft_best.mlmodel $(patsubst splits/%,gen1-print/jobs/_%,$(wildcard splits/x*))
	touch $@

gen1-print/jobs/_%:	splits/%
	mkdir -p $(dir $@)
	$(call gpu_run,cat $< | ./batkraken.sh gen1-print --alto --device cuda:0 segment --model $(segment) -d horizontal-rl --baseline ocr --model $(ocr) --base-dir R >& $@.err)
	touch $@

gen2-print/_SUCCESS:	splits/_SUCCESS $(patsubst splits/%,gen2-print/jobs/_%,$(wildcard splits/x*))
	touch $@

gen2-print/jobs/_%:	splits/%
	mkdir -p $(dir $@)
	$(call slurm_run,cat $< | ./batkraken.sh gen2-print --alto segment --model $(segment) -d horizontal-rl --baseline ocr --model gen1-print-n7m5.out/alto-nall/ft_best.mlmodel --base-dir R >& $@.err)
	touch $@

gen2-%.out/alto-union/alto.lis:	gen2-%.out/lines.json/_SUCCESS gen1-%.out/lines.json/_SUCCESS %.out/lines.json/_SUCCESS
	vtrun patch-alto.py '{,gen1-,gen2-}$*.out/lines.json' $(dir $@)
	find $(dir $@) -type f -name '*.xml' > $@

%.in/_SUCCESS:	%/_SUCCESS
	mkdir -p $(dir $@)
	$(call spark_run,vtrun alto-lines.py $(dir $<) $(dir $@)/ref$(eq)0)
	(cd $(dir $@); ln -s ../docs/ref$(eq)1)
	touch $@

%/ft_best.mlmodel:	%/dataset.arrow
	$(call gpu_run,ketos train --device cuda:0 --load print_transcription_NEW.mlmodel --lrate 0.0001 --output $*/ft --normalization NFD --normalize-whitespace --resize add --fixed-splits --format-type binary $< >& $*/ft.err)

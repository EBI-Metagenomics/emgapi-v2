import logging

# from asgiref.sync import async_to_sync
from django.db.models.signals import post_save, pre_save

# from django.dispatch import receiver
# from prefect.deployments import run_deployment

# import analyses.models

# Define signals (AKA hooks, AKA triggers) here, where they are needed to trigger Prefect work based on
# Django model changes.
# For example, if a flow should being running when a model instance is created.


# @receiver(post_save, sender=analyses.models.AssemblyAnalysisRequest)
# def on_assembly_analysis_saved(
#     sender, instance: analyses.models.AssemblyAnalysisRequest, created, **kwargs
# ):
#     if not created:
#         return
#     flowrun = async_to_sync(run_deployment)(
#         "Assemble and analyse a study/assembly_analysis_request_deployment",
#         timeout=0,
#         parameters={
#             "accession": instance.requested_study,
#             "request_id": instance.id,
#         },
#         idempotency_key=f"assembly_analysis_request_deployment__request_id_{instance.id}",
#     )
#     instance.request_metadata[instance.RequestMetadata.FLOW_RUN_ID] = str(flowrun.id)
#     instance.save()


# Add hooks above.
# The below is largely here to have something in here to call rather than just import it.
# This will stop any linters tidying up an unused import!
# It also might be useful for debugging.


def get_handlers(signal):
    return [h for h in signal.receivers]


def ready():
    pre_save_hooks = get_handlers(pre_save)
    post_save_hooks = get_handlers(post_save)
    logging.debug(f"Hooks are: {pre_save_hooks + post_save_hooks}")
    return pre_save_hooks + post_save_hooks

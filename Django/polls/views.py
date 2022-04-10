from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse
from polls.models import Choice, Question


def index(request):
    q = Question.objects.get(pk=1)
    print(q.choice_set.all())
    # return HttpResponse("Hello, world. You're at the polls index.")
    return HttpResponse(q.choice_set.all())

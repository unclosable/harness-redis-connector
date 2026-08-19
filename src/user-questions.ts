import type { AskUserQuestionItem, AskUserQuestionAnswer } from '@deepseek-ai/dsh-user-questions'
import type { OutboundQuestion } from './protocol/types.js'

export function shouldRouteToRedis(title: string | undefined): boolean {
  return title?.startsWith('bot-') === true
}

export function mapQuestionsToOutbound(questions: AskUserQuestionItem[]): OutboundQuestion[] {
  return questions.map(q => {
    const out: OutboundQuestion = {
      id: q.id,
      text: q.question,
    }
    if (q.options && q.options.length > 0) out.options = q.options.map(o => o.label)
    if (q.multiSelect) out.multi_select = true
    return out
  })
}

// Map Flink answer envelope content -> AskUserQuestionAnswer
export function mapFlinkAnswersToAskAnswer(answersRaw: { id: string; value: string | string[]; custom?: string }[]): AskUserQuestionAnswer {
  return {
    answers: answersRaw.map(a => ({ id: a.id, selected: Array.isArray(a.value) ? [...a.value] : [a.value], ...(typeof a.custom === 'string' ? { custom: a.custom } : {}) })),
  }
}

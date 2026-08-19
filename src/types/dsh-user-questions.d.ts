declare module '@deepseek-ai/dsh-user-questions' {
  export interface AskUserQuestionOption { label: string; description?: string }
  export interface AskUserQuestionItem { id: string; question: string; detail?: string; header?: string; options?: AskUserQuestionOption[]; multiSelect?: boolean }
  export interface AskUserQuestionAnswerItem { id: string; selected: string[]; custom?: string }
  export interface AskUserQuestionAnswer { answers: AskUserQuestionAnswerItem[] }
  export interface AskUserQuestionRequest { questions: AskUserQuestionItem[]; agent?: unknown; signal?: AbortSignal }
  export interface UserQuestionRouter {
    ask(request: AskUserQuestionRequest, next: (req: AskUserQuestionRequest) => Promise<AskUserQuestionAnswer>): Promise<AskUserQuestionAnswer>
  }
  export type AskUserQuestionRequest = AskUserQuestionRequest
  export type AskUserQuestionAnswer = AskUserQuestionAnswer
  export type UserQuestionRouter = UserQuestionRouter
}

import { Server, Socket } from 'socket.io';
import { PrismaClient } from '@prisma/client';
import { roomManager } from './roomManager';
import { getSessionByPin, getLeaderboard, getFullLeaderboard } from '../services/sessionService';
import { calculateScore, isAnswerCorrect } from '../services/scoringService';

const prisma = new PrismaClient();
const queues = new Map<string, Promise<void>>();
const timers = new Map<string, ReturnType<typeof setTimeout>>();

function enqueue(pin: string, task: () => Promise<void>): Promise<void> {
  const next = (queues.get(pin) ?? Promise.resolve()).then(task);
  const settled = next.catch(() => {});
  queues.set(pin, settled);
  void settled.then(() => { if (queues.get(pin) === settled) queues.delete(pin); });
  return next;
}

function clearTimer(pin: string) {
  const timer = timers.get(pin);
  if (timer) clearTimeout(timer);
  timers.delete(pin);
}

function isPoll(question: any): boolean {
  return question.type === 'OPEN_TEXT' || question.type === 'SLIDER' ||
    (question.type === 'MULTIPLE_CHOICE' && !(question.options as any[]).some(o => o.isCorrect));
}

async function closeQuestion(io: Server, pin: string) {
  const room = roomManager.get(pin);
  if (!room || room.status !== 'active') return;
  clearTimer(pin);
  const session = await getSessionByPin(pin);
  const question = session?.quiz.questions[room.currentQ];
  if (!question) return;
  if (!isPoll(question)) {
    await prisma.participant.updateMany({
      where: { sessionId: room.sessionId, answers: { none: { questionId: question.id } } },
      data: { streak: 0 },
    });
  }
  roomManager.update(pin, { status: 'between' });
  io.to(pin).emit('game:question_closed', { index: room.currentQ, questionId: question.id });
}

function armTimer(io: Server, pin: string, timeLimit: number) {
  clearTimer(pin);
  const room = roomManager.get(pin);
  if (!room) return;
  const startedAt = room.questionStartedAt;
  const index = room.currentQ;
  const sessionId = room.sessionId;
  const expire = () => enqueue(pin, async () => {
    const current = roomManager.get(pin);
    if (!current || current.sessionId !== sessionId || current.currentQ !== index ||
        current.questionStartedAt !== startedAt || current.status !== 'active') return;
    await closeQuestion(io, pin);
  }).catch(error => {
    console.error('[ws] question timeout failed', error);
    const current = roomManager.get(pin);
    if (current?.sessionId === sessionId && current.currentQ === index &&
        current.questionStartedAt === startedAt && current.status === 'active') {
      timers.set(pin, setTimeout(expire, 1000));
    }
  });
  timers.set(pin, setTimeout(expire, Math.max(0, startedAt + timeLimit * 1000 - Date.now())));
}

export function registerEvents(io: Server, socket: Socket) {
  function onRoom(event: string, handler: (data: any, receivedAt: number) => Promise<void>, player = false) {
    socket.on(event, (data: any = {}) => {
      const receivedAt = Date.now();
      const pin = player ? socket.data.pin : data?.pin;
      if (typeof pin !== 'string') return;
      void enqueue(pin, () => handler(data, receivedAt)).catch(error => {
        console.error(`[ws] ${event} failed`, error);
        socket.emit('error', { msg: 'Unable to process request. Please try again.' });
      });
    });
  }

  socket.on('spectator:join', ({ pin }: { pin: string }) => {
    const room = roomManager.get(pin);
    if (!room) return socket.emit('error', { msg: 'Room not found' });
    socket.join(pin);
    socket.emit('spectator:joined', { status: room.status, currentQ: room.currentQ, totalParticipants: room.totalParticipants });
  });

  onRoom('host:join', async ({ pin, maxParticipants }) => {
    const session = await getSessionByPin(pin);
    if (!session) { socket.emit('error', { msg: 'Session not found' }); return; }
    socket.join(pin);
    const existing = roomManager.get(pin);
    if (existing && existing.sessionId === session.id) {
      roomManager.update(pin, { hostSocketId: socket.id });
    } else {
      clearTimer(pin);
      roomManager.create(pin, session.id, socket.id, maxParticipants ?? 0);
    }
    socket.emit('host:joined', { session });
  });

  onRoom('player:join', async ({ pin, nickname, avatar }) => {
    const room = roomManager.get(pin);
    if (!room) { socket.emit('error', { msg: 'Room not found' }); return; }
    if (room.status !== 'waiting') { socket.emit('error', { msg: 'Game already started' }); return; }
    if (room.maxParticipants > 0 && room.totalParticipants >= room.maxParticipants) {
      socket.emit('error', { msg: 'Room is full' }); return;
    }
    const participant = await prisma.participant.create({ data: { sessionId: room.sessionId, nickname, avatar } });
    socket.join(pin);
    socket.data.participantId = participant.id;
    socket.data.pin = pin;
    roomManager.update(pin, { totalParticipants: room.totalParticipants + 1 });
    socket.emit('player:joined', { participant });
    io.to(pin).emit('room:player_joined', { participant: { id: participant.id, nickname, avatar }, total: room.totalParticipants });
  });

  onRoom('host:start', async ({ pin }) => {
    const room = roomManager.get(pin);
    if (!room || room.hostSocketId !== socket.id || room.status !== 'waiting') return;
    const session = await getSessionByPin(pin);
    const question = session?.quiz.questions[0];
    if (!session || !question) return;
    await prisma.quizSession.update({ where: { id: room.sessionId }, data: { status: 'ACTIVE', startedAt: new Date() } });
    roomManager.update(pin, { status: 'active', currentQ: 0, questionStartedAt: Date.now(), answerCount: 0 });
    armTimer(io, pin, question.timeLimit);
    io.to(pin).emit('game:start');
    io.to(pin).emit('game:question', { index: 0, total: session.quiz.questions.length, question: stripCorrectAnswers(question), timeLimit: question.timeLimit });
  });

  onRoom('player:answer', async ({ value }, receivedAt) => {
    const { participantId, pin } = socket.data;
    if (!participantId || !pin) return;
    const room = roomManager.get(pin);
    if (!room || room.status !== 'active') { socket.emit('error', { msg: 'Question is closed' }); return; }
    const session = await getSessionByPin(pin);
    const question = session?.quiz.questions[room.currentQ];
    if (!question) return;
    const responseTimeMs = receivedAt - room.questionStartedAt;
    if (responseTimeMs < 0 || responseTimeMs >= question.timeLimit * 1000) {
      await closeQuestion(io, pin);
      socket.emit('error', { msg: 'Time is up. Answer not accepted.' });
      return;
    }
    const participant = await prisma.participant.findUnique({ where: { id: participantId } });
    if (!participant || participant.sessionId !== room.sessionId) return;
    const previous = await prisma.answer.findUnique({ where: { participantId_questionId: { participantId, questionId: question.id } } });
    if (previous) { socket.emit('error', { msg: 'Answer already submitted' }); return; }
    const correctResult = isAnswerCorrect(question.type, value, question.options as any);
    const poll = correctResult === null;
    const correct = correctResult === true;
    const { pointsEarned, newStreak, comboBonus, timeBonus } = poll
      ? { pointsEarned: 0, newStreak: participant.streak, comboBonus: 0, timeBonus: 0 }
      : calculateScore(correct, responseTimeMs, question.timeLimit * 1000, participant.streak, question.pointsBase);
    await prisma.$transaction([
      prisma.answer.create({ data: { participantId, questionId: question.id, value: value as any, isCorrect: correct, pointsEarned, responseTimeMs } }),
      prisma.participant.update({ where: { id: participantId }, data: { score: { increment: pointsEarned }, streak: newStreak } }),
    ]);
    roomManager.update(pin, { answerCount: room.answerCount + 1 });
    socket.emit('player:answer_result', { correct: poll ? null : correct, pointsEarned, comboBonus, timeBonus, newStreak });
    io.to(pin).emit('room:answer_count', { count: room.answerCount, total: room.totalParticipants });
    io.to(room.hostSocketId).emit('host:answer_count', { count: room.answerCount, total: room.totalParticipants });
    if (poll || question.type === 'TRUE_FALSE' || question.type === 'PUZZLE') {
      const answers = await prisma.answer.findMany({ where: { questionId: question.id, participant: { sessionId: room.sessionId } }, select: { value: true, isCorrect: true } });
      const base = { type: question.type, totalAnswers: answers.length };
      if (question.type === 'MULTIPLE_CHOICE' || question.type === 'TRUE_FALSE') {
        const counts: Record<string, number> = {};
        for (const a of answers) {
          const ids = Array.isArray(a.value) ? a.value as string[] : [String(a.value)];
          for (const id of ids) counts[id] = (counts[id] || 0) + 1;
        }
        io.to(pin).emit('room:vote_update', { ...base, distribution: (question.options as any[]).map(o => ({ id: o.id, text: o.text, count: counts[o.id] || 0 })) });
      } else if (question.type === 'OPEN_TEXT') {
        const freq: Record<string, number> = {};
        for (const a of answers) { const word = String(a.value ?? '').toLowerCase().trim(); if (word) freq[word] = (freq[word] || 0) + 1; }
        io.to(pin).emit('room:vote_update', { ...base, wordFrequency: freq });
      } else if (question.type === 'SLIDER') {
        const vals = answers.map(a => Number(a.value)).filter(v => !isNaN(v));
        io.to(pin).emit('room:vote_update', { ...base, totalAnswers: vals.length, avg: vals.length ? Math.round(vals.reduce((s, v) => s + v, 0) / vals.length) : 0, min: vals.length ? Math.min(...vals) : 0, max: vals.length ? Math.max(...vals) : 0 });
      } else if (question.type === 'PUZZLE') {
        const correctCount = answers.filter(a => a.isCorrect).length;
        io.to(pin).emit('room:vote_update', { ...base, correctCount, incorrectCount: answers.length - correctCount });
      }
    }
  }, true);

  onRoom('host:next', async ({ pin }) => {
    const room = roomManager.get(pin);
    if (!room || room.hostSocketId !== socket.id || (room.status !== 'active' && room.status !== 'between')) return;
    const session = await getSessionByPin(pin);
    if (!session) return;
    await closeQuestion(io, pin);
    io.to(pin).emit('game:leaderboard', { leaderboard: await getLeaderboard(room.sessionId) });
    const nextIndex = room.currentQ + 1;
    if (nextIndex >= session.quiz.questions.length) {
      await prisma.quizSession.update({ where: { id: room.sessionId }, data: { status: 'FINISHED', endedAt: new Date() } });
      clearTimer(pin);
      roomManager.update(pin, { status: 'finished' });
      io.to(pin).emit('game:end', { leaderboard: await getFullLeaderboard(room.sessionId) });
      return;
    }
    const question = session.quiz.questions[nextIndex];
    await prisma.quizSession.update({ where: { id: room.sessionId }, data: { currentQ: nextIndex, status: 'ACTIVE' } });
    roomManager.update(pin, { currentQ: nextIndex, questionStartedAt: Date.now(), answerCount: 0, status: 'active' });
    armTimer(io, pin, question.timeLimit);
    io.to(pin).emit('game:question', { index: nextIndex, total: session.quiz.questions.length, question: stripCorrectAnswers(question), timeLimit: question.timeLimit });
  });

  onRoom('host:end', async ({ pin }) => {
    const room = roomManager.get(pin);
    if (!room || room.hostSocketId !== socket.id || room.status === 'finished') return;
    await closeQuestion(io, pin);
    await prisma.quizSession.update({ where: { id: room.sessionId }, data: { status: 'FINISHED', endedAt: new Date() } });
    clearTimer(pin);
    roomManager.update(pin, { status: 'finished' });
    io.to(pin).emit('game:end', { leaderboard: await getFullLeaderboard(room.sessionId) });
  });

  socket.on('disconnect', () => {
    const room = roomManager.findBySocketId(socket.id);
    if (room) io.to(room.pin).emit('host:disconnected');
  });
}

function stripCorrectAnswers(question: any) {
  const { options, ...rest } = question;
  return { ...rest, options: (options as any[]).map(({ id, text }: any) => ({ id, text })) };
}

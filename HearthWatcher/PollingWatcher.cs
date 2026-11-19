using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace HearthWatcher;

public abstract class PollingWatcher
{
	private static SynchronizationContext? _eventContext;
	private static int _eventThreadId = -1;

	// call once from the UI thread during app startup, before any watcher runs
	public static void SetEventContext(SynchronizationContext? ctx)
	{
		_eventContext = ctx;
		_eventThreadId = ctx != null ? Environment.CurrentManagedThreadId : -1;
	}

	#region Adaptive polling

	// Every tick is a cross-process read of Hearthstone's memory, so polling at the base rate when
	// nothing is changing is pure overhead. A watcher that has not raised an event for IdleAfterMs
	// drops to its idle delay and snaps back to the base delay on the next change.
	public const int IdleAfterMs = 2000;
	internal const int IdleDelayMultiplier = 3;
	internal const int MaxIdleDelay = 100;

	// While Hearthstone is not in the foreground (or minimized) all watchers poll at this rate at most.
	public const int ThrottledDelay = 250;

	private static readonly Stopwatch Clock = Stopwatch.StartNew();
	private static volatile bool _throttled;
	private static volatile CancellationTokenSource _wakeCts = new();
	private static readonly object WakeLock = new();

	public static bool IsThrottled => _throttled;

	/// <summary>
	/// Slow every watcher down to <see cref="ThrottledDelay"/>, e.g. while Hearthstone is in the
	/// background. Un-throttling wakes all sleeping watchers immediately, so returning to the game
	/// does not wait out a throttled delay.
	/// </summary>
	public static void SetThrottled(bool throttled)
	{
		if(_throttled == throttled)
			return;
		_throttled = throttled;
		if(!throttled)
			WakeAll();
	}

	private static void WakeAll()
	{
		CancellationTokenSource old;
		lock(WakeLock)
		{
			old = _wakeCts;
			_wakeCts = new CancellationTokenSource();
		}
		// Not disposed: a watcher may still be about to pass old.Token to Task.Delay, which is fine
		// for a cancelled source but not for a disposed one. It holds no unmanaged resources.
		old.Cancel();
	}

	private long _lastActivityMs;

	private void MarkActive() => Interlocked.Exchange(ref _lastActivityMs, Clock.ElapsedMilliseconds);

	private int CurrentDelay()
	{
		if(_throttled)
			return Math.Max(_delay, ThrottledDelay);
		var idleFor = Clock.ElapsedMilliseconds - Interlocked.Read(ref _lastActivityMs);
		return idleFor > IdleAfterMs ? _idleDelay : _delay;
	}

	private static async Task WaitAsync(int delay)
	{
		if(delay < ThrottledDelay)
		{
			await Task.Delay(delay).ConfigureAwait(false);
			return;
		}
		try
		{
			await Task.Delay(delay, _wakeCts.Token).ConfigureAwait(false);
		}
		catch(OperationCanceledException)
		{
			// woken up early by SetThrottled(false)
		}
	}

	#endregion

	private readonly object _stateLock = new();
	private readonly int _delay;
	private readonly int _idleDelay;
	private bool _watch;
	private Task? _loop;

	protected PollingWatcher(int delay)
	{
		_delay = delay;
		_idleDelay = Math.Max(delay, Math.Min(delay * IdleDelayMultiplier, MaxIdleDelay));
	}

	public void Run()
	{
		lock(_stateLock)
		{
			_watch = true;
			_loop ??= Task.Run(RunLoopAsync);
		}
	}

	public void Stop()
	{
		lock(_stateLock)
			_watch = false;
	}

	private bool Watching
	{
		get
		{
			lock(_stateLock)
				return _watch;
		}
	}

	private async Task RunLoopAsync()
	{
		while(true)
		{
			OnLoopStart();
			// start at the base rate, the first reads after (re)starting are the interesting ones
			MarkActive();
			while(Watching)
			{
				await WaitAsync(CurrentDelay()).ConfigureAwait(false);
				if(!Watching)
					break;
				if(await TickAsync().ConfigureAwait(false))
				{
					Stop();
					break;
				}
			}
			OnLoopEnd();
			lock(_stateLock)
			{
				if(!_watch)
				{
					_loop = null;
					return;
				}
				// Run() landed while we were draining, restart with fresh state
			}
		}
	}

	/// <summary>
	/// One poll iteration, runs on a thread-pool thread. Return true to stop the loop.
	/// </summary>
	protected abstract Task<bool> TickAsync();

	protected virtual void OnLoopStart()
	{
	}

	protected virtual void OnLoopEnd()
	{
	}

	// raise an event on the UI thread, inline when already there (compare thread ids,
	// WPF hands out distinct SynchronizationContext instances per operation).
	// Raising an event also counts as activity and keeps the watcher at its base rate.
	protected void Dispatch(Action raise)
	{
		MarkActive();
		var ctx = _eventContext;
		if(ctx == null || Environment.CurrentManagedThreadId == _eventThreadId)
			raise();
		else
			ctx.Post(_ => raise(), null);
	}
}
